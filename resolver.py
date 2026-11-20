"""Row-level conflict resolver for multi-master database replication.

Deterministic, wall-clock-free merge of row updates based on version
vectors. Compatible with Python 3.8.2+.
"""

from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple

DOMINATES = "dominates"
DOMINATED = "dominated"
EQUAL = "equal"
CONCURRENT = "concurrent"

MERGED = "merged"
DELETED = "deleted"
CONFLICT = "conflict"
QUARANTINED = "quarantined"
NOOP = "noop"


def compare_vectors(a, b):
    # type: (Dict[str, int], Dict[str, int]) -> str
    """Compare two version vectors without any wall-clock input."""
    nodes = set(a) | set(b)
    a_gt = any(a.get(n, 0) > b.get(n, 0) for n in nodes)
    b_gt = any(b.get(n, 0) > a.get(n, 0) for n in nodes)
    if a_gt and b_gt:
        return CONCURRENT
    if a_gt:
        return DOMINATES
    if b_gt:
        return DOMINATED
    return EQUAL


def merge_vectors(vectors):
    # type: (Tuple[Dict[str, int], ...]) -> Dict[str, int]
    merged = {}  # type: Dict[str, int]
    for vec in vectors:
        for node, counter in vec.items():
            if counter > merged.get(node, 0):
                merged[node] = counter
    return merged


def _freeze(value):
    """Order-independent, hashable representation of a column value."""
    return (type(value).__name__, repr(value))


@dataclass(frozen=True)
class RowUpdate:
    """A single replicated write to one row.

    ``columns`` holds only the columns written by this update (a delta),
    not a full row snapshot. ``tombstone`` marks a delete.
    """

    pk: Any
    columns: Tuple[Tuple[str, Any], ...]
    vector: Tuple[Tuple[str, int], ...]
    origin: str
    tombstone: bool = False

    @staticmethod
    def make(pk, columns, vector, origin, tombstone=False):
        # type: (Any, Dict[str, Any], Dict[str, int], str, bool) -> RowUpdate
        return RowUpdate(
            pk=pk,
            columns=tuple(sorted(columns.items(), key=lambda kv: kv[0])),
            vector=tuple(sorted(vector.items())),
            origin=origin,
            tombstone=tombstone,
        )

    @property
    def column_map(self):
        # type: () -> Dict[str, Any]
        return dict(self.columns)

    @property
    def vector_map(self):
        # type: () -> Dict[str, int]
        return dict(self.vector)

    def rank(self):
        """Deterministic total order key; independent of input order."""
        return (self.origin, self.vector, self.tombstone,
                tuple((k, _freeze(v)) for k, v in self.columns))


@dataclass(frozen=True)
class Constraints:
    not_null: Tuple[str, ...] = ()
    types: Tuple[Tuple[str, type], ...] = ()
    unique_keys: Tuple[Tuple[str, ...], ...] = ()

    @staticmethod
    def make(not_null=(), types=None, unique_keys=()):
        return Constraints(
            not_null=tuple(sorted(not_null)),
            types=tuple(sorted((types or {}).items())),
            unique_keys=tuple(sorted(tuple(k) for k in unique_keys)),
        )

    @property
    def type_map(self):
        return dict(self.types)


@dataclass(frozen=True)
class ConflictRecord:
    kind: str  # "column" or "delete_update"
    column: Optional[str]
    contenders: Tuple[Tuple[str, str], ...]  # (origin, repr of value/delete)
    resolution: str


@dataclass(frozen=True)
class Resolution:
    status: str
    pk: Any
    row: Optional[Tuple[Tuple[str, Any], ...]]
    vector: Tuple[Tuple[str, int], ...]
    conflicts: Tuple[ConflictRecord, ...] = ()
    reason: Optional[str] = None

    @property
    def row_map(self):
        return dict(self.row) if self.row is not None else None

    @property
    def vector_map(self):
        return dict(self.vector)


def _dedupe(updates):
    """Drop exact replays (same origin re-sending the same write)."""
    seen = {}
    for upd in updates:
        seen[upd.rank()] = upd
    return tuple(seen[k] for k in sorted(seen))


def _maximal(updates):
    """Keep only updates not causally dominated by any other update."""
    return tuple(
        upd for upd in updates
        if not any(other is not upd and _dominates(other, upd)
                   for other in updates)
    )


def _dominates(a, b):
    return compare_vectors(a.vector_map, b.vector_map) == DOMINATES


def _validate(row, constraints, existing_rows, pk):
    """Return a sorted tuple of constraint violation messages."""
    violations = []
    for col in constraints.not_null:
        if row.get(col) is None:
            violations.append("not_null:%s" % col)
    for col, expected in constraints.types:
        if col in row and row[col] is not None:
            value = row[col]
            ok = isinstance(value, expected)
            if expected is int and isinstance(value, bool):
                ok = False
            if not ok:
                violations.append(
                    "type:%s:expected_%s" % (col, expected.__name__))
    for key in constraints.unique_keys:
        for other in existing_rows:
            if other.get("pk") == pk:
                continue
            if all(other.get(col) == row.get(col) for col in key):
                violations.append("unique:%s" % ",".join(key))
                break
    return tuple(sorted(violations))


def _finish(pk, row, vector, conflicts, constraints, existing_rows):
    violations = _validate(row, constraints, existing_rows, pk)
    status = CONFLICT if conflicts else MERGED
    if violations:
        return Resolution(
            status=QUARANTINED,
            pk=pk,
            row=None,
            vector=tuple(sorted(vector.items())),
            conflicts=conflicts,
            reason=";".join(violations),
        )
    return Resolution(
        status=status,
        pk=pk,
        row=tuple(sorted(row.items(), key=lambda kv: kv[0])),
        vector=tuple(sorted(vector.items())),
        conflicts=conflicts,
    )


def resolve(updates, base=None, constraints=None, existing_rows=()):
    # type: (...) -> Resolution
    """Resolve a set of replicated updates for one row.

    ``updates``: iterable of RowUpdate for the same primary key.
    ``base``: current local row as a dict (columns absent from every
        update keep their base value). None means the row does not exist.
    ``constraints``: Constraints instance.
    ``existing_rows``: other rows (dicts, each with a "pk" key) used for
        unique-key checks.

    The result depends only on the update contents, never on their
    order, on wall-clock time, or on which node calls resolve().

    Causality is tracked per column: an update only overwrites the
    columns it actually wrote, so a later update from another node does
    not discard concurrent or older writes to columns it never touched.
    Tombstones are row-level: if a delete is maximal (no write causally
    follows it) the row is removed; if a write causally follows every
    delete, the row is resurrected from the post-delete writes only.
    """
    constraints = constraints or Constraints.make()
    base = dict(base) if base else {}
    updates = _dedupe(list(updates))
    if not updates:
        return Resolution(
            status=NOOP,
            pk=base.get("pk"),
            row=tuple(sorted(base.items(), key=lambda kv: kv[0])) or None,
            vector=(),
        )
    pk = updates[0].pk
    maximal = _maximal(updates)
    vector = merge_vectors(tuple(u.vector_map for u in maximal))

    tombstones = tuple(u for u in updates if u.tombstone)
    writes = tuple(u for u in updates if not u.tombstone)
    conflicts = []

    live_tombstones = tuple(u for u in maximal if u.tombstone)
    if live_tombstones:
        # Tombstone rule: a delete concurrent with (or dominating) an
        # update removes the row. Remove-wins, decided purely by version
        # vectors; an update that causally follows the tombstone would
        # have dominated it and the tombstone would not be maximal.
        live_writes = tuple(u for u in maximal if not u.tombstone)
        if live_writes:
            contenders = tuple(sorted(
                [(u.origin, "delete") for u in live_tombstones]
                + [(u.origin, "update") for u in live_writes]))
            conflicts.append(ConflictRecord(
                kind="delete_update",
                column=None,
                contenders=contenders,
                resolution="delete_wins",
            ))
        return Resolution(
            status=DELETED,
            pk=pk,
            row=None,
            vector=tuple(sorted(vector.items())),
            conflicts=tuple(conflicts),
        )

    if tombstones:
        # Every delete was causally superseded by a later write: the row
        # was deleted and re-created. Pre-delete writes and base columns
        # were removed by the delete and must not be resurrected.
        writes = tuple(
            w for w in writes
            if not any(_dominates(t, w) for t in tombstones))
        base = {"pk": pk} if "pk" in base else {}

    # Merge writes column by column. For each column, only the updates
    # that wrote it participate, and causally overwritten writes to that
    # column are discarded (late-arriving stale values cannot win).
    merged = dict(base)
    by_column = {}
    for upd in writes:
        for col, value in upd.columns:
            by_column.setdefault(col, []).append((upd, value))
    for col in sorted(by_column):
        entries = [
            (upd, value) for upd, value in by_column[col]
            if not any(_dominates(other, upd)
                       for other, _ in by_column[col])
        ]
        distinct = {}
        for upd, value in entries:
            distinct.setdefault(_freeze(value), []).append(upd)
        if len(distinct) == 1:
            merged[col] = entries[0][1]
            continue
        # Same-column conflict: deterministic winner by update rank,
        # recorded explicitly instead of silently picking a side.
        winner_upd, winner_val = min(
            entries, key=lambda e: (e[0].rank(), _freeze(e[1])))
        merged[col] = winner_val
        contenders = tuple(sorted(
            (u.origin, repr(v)) for u, v in entries))
        conflicts.append(ConflictRecord(
            kind="column",
            column=col,
            contenders=contenders,
            resolution="deterministic_winner:%s" % winner_upd.origin,
        ))
    return _finish(pk, merged, vector, tuple(conflicts),
                   constraints, existing_rows)
