import itertools
import unittest

from resolver import (
    CONCURRENT,
    CONFLICT,
    DELETED,
    DOMINATES,
    MERGED,
    QUARANTINED,
    Constraints,
    RowUpdate,
    compare_vectors,
    resolve,
)

BASE = {"pk": 1, "name": "alice", "age": 30, "email": "alice@example.com"}

USER_CONSTRAINTS = Constraints.make(
    not_null=("name",),
    types={"name": str, "age": int, "email": str},
    unique_keys=(("email",),),
)


def upd(columns, vector, origin, tombstone=False):
    return RowUpdate.make(pk=1, columns=columns, vector=vector,
                          origin=origin, tombstone=tombstone)


class VersionVectorTest(unittest.TestCase):
    def test_compare(self):
        self.assertEqual(compare_vectors({"a": 1}, {"a": 1}), "equal")
        self.assertEqual(compare_vectors({"a": 2}, {"a": 1}), DOMINATES)
        self.assertEqual(compare_vectors({"a": 1, "b": 1}, {"a": 1}),
                         DOMINATES)
        self.assertEqual(compare_vectors({"a": 1}, {"b": 1}), CONCURRENT)
        self.assertEqual(compare_vectors({"a": 2, "b": 1}, {"a": 1, "b": 2}),
                         CONCURRENT)


class StaleLateArrivalTest(unittest.TestCase):
    def test_causally_overwritten_value_is_not_resurrected(self):
        stale = upd({"name": "old-name"}, {"a": 1}, "a")
        newer = upd({"name": "new-name"}, {"a": 2}, "a")
        # Stale update arrives after the newer one.
        res = resolve([newer, stale], base=BASE)
        self.assertEqual(res.status, MERGED)
        self.assertEqual(res.row_map["name"], "new-name")
        self.assertEqual(res.conflicts, ())
        self.assertEqual(res.vector_map, {"a": 2})


class ConcurrentMergeTest(unittest.TestCase):
    def test_different_columns_merge(self):
        u1 = upd({"name": "bob"}, {"a": 1}, "a")
        u2 = upd({"age": 31}, {"b": 1}, "b")
        res = resolve([u1, u2], base=BASE, constraints=USER_CONSTRAINTS)
        self.assertEqual(res.status, MERGED)
        self.assertEqual(res.row_map["name"], "bob")
        self.assertEqual(res.row_map["age"], 31)
        self.assertEqual(res.row_map["email"], "alice@example.com")
        self.assertEqual(res.conflicts, ())
        self.assertEqual(res.vector_map, {"a": 1, "b": 1})

    def test_same_value_same_column_is_not_a_conflict(self):
        u1 = upd({"name": "bob"}, {"a": 1}, "a")
        u2 = upd({"name": "bob"}, {"b": 1}, "b")
        res = resolve([u1, u2], base=BASE, constraints=USER_CONSTRAINTS)
        self.assertEqual(res.status, MERGED)
        self.assertEqual(res.row_map["name"], "bob")
        self.assertEqual(res.conflicts, ())


class SameColumnConflictTest(unittest.TestCase):
    def test_conflict_record_and_deterministic_winner(self):
        u1 = upd({"name": "bob"}, {"a": 1}, "a")
        u2 = upd({"name": "carol"}, {"b": 1}, "b")
        res = resolve([u1, u2], base=BASE, constraints=USER_CONSTRAINTS)
        self.assertEqual(res.status, CONFLICT)
        self.assertEqual(len(res.conflicts), 1)
        rec = res.conflicts[0]
        self.assertEqual(rec.kind, "column")
        self.assertEqual(rec.column, "name")
        self.assertEqual(rec.contenders,
                         (("a", "'bob'"), ("b", "'carol'")))
        # Deterministic winner: lowest rank (origin "a").
        self.assertEqual(res.row_map["name"], "bob")

    def test_input_order_does_not_change_result(self):
        u1 = upd({"name": "bob"}, {"a": 1}, "a")
        u2 = upd({"name": "carol"}, {"b": 1}, "b")
        results = set()
        for perm in itertools.permutations([u1, u2]):
            res = resolve(list(perm), base=BASE, constraints=USER_CONSTRAINTS)
            results.add(res)
        self.assertEqual(len(results), 1)


class DeleteUpdateTest(unittest.TestCase):
    def test_concurrent_delete_and_update_delete_wins(self):
        delete = upd({}, {"a": 1}, "a", tombstone=True)
        write = upd({"age": 31}, {"b": 1}, "b")
        res = resolve([delete, write], base=BASE,
                      constraints=USER_CONSTRAINTS)
        self.assertEqual(res.status, DELETED)
        self.assertIsNone(res.row)
        self.assertEqual(len(res.conflicts), 1)
        self.assertEqual(res.conflicts[0].kind, "delete_update")
        self.assertEqual(res.conflicts[0].resolution, "delete_wins")

    def test_dominating_tombstone_deletes_without_conflict(self):
        write = upd({"age": 31}, {"a": 1}, "a")
        delete = upd({}, {"a": 2}, "a", tombstone=True)
        res = resolve([write, delete], base=BASE)
        self.assertEqual(res.status, DELETED)
        self.assertEqual(res.conflicts, ())

    def test_update_after_tombstone_resurrects_row(self):
        delete = upd({}, {"a": 1}, "a", tombstone=True)
        write = upd({"name": "alice", "age": 31}, {"a": 1, "b": 1}, "b")
        res = resolve([delete, write], base=BASE,
                      constraints=USER_CONSTRAINTS)
        self.assertEqual(res.status, MERGED)
        self.assertEqual(res.row_map["age"], 31)


class ConstraintTest(unittest.TestCase):
    def test_unique_key_violation_goes_to_quarantine(self):
        existing = [{"pk": 2, "name": "mallory",
                     "email": "taken@example.com"}]
        u1 = upd({"email": "taken@example.com"}, {"a": 1}, "a")
        res = resolve([u1], base=BASE, constraints=USER_CONSTRAINTS,
                      existing_rows=existing)
        self.assertEqual(res.status, QUARANTINED)
        self.assertIsNone(res.row)
        self.assertIn("unique:email", res.reason)

    def test_not_null_violation_goes_to_quarantine(self):
        u1 = upd({"name": None}, {"a": 1}, "a")
        res = resolve([u1], base=BASE, constraints=USER_CONSTRAINTS)
        self.assertEqual(res.status, QUARANTINED)
        self.assertIn("not_null:name", res.reason)

    def test_type_violation_goes_to_quarantine(self):
        u1 = upd({"age": "thirty-one"}, {"a": 1}, "a")
        res = resolve([u1], base=BASE, constraints=USER_CONSTRAINTS)
        self.assertEqual(res.status, QUARANTINED)
        self.assertIn("type:age:expected_int", res.reason)

    def test_conflicting_merge_that_breaks_constraints_is_quarantined(self):
        # Same-column conflict whose deterministic winner violates NOT NULL.
        u1 = upd({"name": None}, {"a": 1}, "a")
        u2 = upd({"name": "carol"}, {"b": 1}, "b")
        res = resolve([u1, u2], base=BASE, constraints=USER_CONSTRAINTS)
        self.assertEqual(res.status, QUARANTINED)
        self.assertEqual(len(res.conflicts), 1)
        self.assertIn("not_null:name", res.reason)


class ReplayAndOrderTest(unittest.TestCase):
    def test_replay_and_permutation_are_idempotent(self):
        u1 = upd({"name": "bob"}, {"a": 1}, "a")
        u2 = upd({"age": 31}, {"b": 1}, "b")
        u3 = upd({"email": "bob@example.com"}, {"a": 1, "c": 1}, "c")
        updates = [u1, u2, u3]
        expected = resolve(updates, base=BASE, constraints=USER_CONSTRAINTS)
        for perm in itertools.permutations(updates):
            # Each node replays its write; duplicates must not matter.
            replayed = list(perm) + list(perm)
            res = resolve(replayed, base=BASE, constraints=USER_CONSTRAINTS)
            self.assertEqual(res, expected)
        self.assertEqual(expected.status, MERGED)
        self.assertEqual(expected.row_map["name"], "bob")
        self.assertEqual(expected.row_map["age"], 31)
        self.assertEqual(expected.row_map["email"], "bob@example.com")
        self.assertEqual(expected.vector_map, {"a": 1, "b": 1, "c": 1})


if __name__ == "__main__":
    unittest.main()
