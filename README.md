# database-replication-conflict-resolver

解析多主复制冲突并生成可审计的合并与回退方案。

行级复制冲突解析器（Python 3.8.2+，仅标准库）。针对双主/多主数据库
同时更新同一行的场景，用 version vector 区分因果覆盖与并发写入，
不依赖 wall clock（服务器时间）。

## 用法

```python
from resolver import Constraints, RowUpdate, resolve

updates = [
    RowUpdate.make(pk=1, columns={"name": "bob"}, vector={"a": 1}, origin="a"),
    RowUpdate.make(pk=1, columns={"age": 31}, vector={"b": 1}, origin="b"),
]
constraints = Constraints.make(
    not_null=("name",),
    types={"name": str, "age": int},
    unique_keys=(("email",),),
)
res = resolve(updates, base=current_row, constraints=constraints,
              existing_rows=other_rows)
res.status      # "merged" | "conflict" | "deleted" | "quarantined" | "noop"
res.row_map     # 合并后的行（deleted/quarantined 时为 None）
res.conflicts   # 明确的 ConflictRecord 列表
res.reason      # quarantine 原因
```

## 合并策略

- **因果覆盖**：version vector 可比较时，被支配（更旧）的写入直接丢弃，
  晚到的旧值不会复活。因果判断按列进行：一个更新只覆盖它实际写过的列。
- **并发写不同列**：取各更新所写列的并集合并，未触及的列保留 base 值。
- **并发写同一列**：生成 `ConflictRecord(kind="column")`，并按确定性的
  全序（origin + version vector，与输入顺序无关）选出胜者，绝不静默选边。
- **删除与更新并发**：按墓碑规则 remove-wins，状态为 `deleted` 并生成
  `ConflictRecord(kind="delete_update")`；因果上晚于墓碑的更新会使行复活
  （仅由删除之后的写重建，删除前的列值不复活）。
- **约束校验**：合并结果必须通过非空、类型和唯一键约束；无法满足时进入
  `quarantined`（携带原因与冲突记录），而不是静默选择一侧。
- **确定性**：相同输入与相同 version vector 必产生相同结果；节点重放
  （重复投递）与输入顺序不影响最终值。

## 重要说明

合并策略属于**业务规则**：`resolve()` 只计算合并结果、冲突记录与
quarantine 决定，**不自动提交任何数据库事务**。是否应用合并结果、
如何补偿或人工介入 quarantine 记录，由调用方在自己的事务边界内决定。

## 测试

```
python -m unittest
```

覆盖：旧值晚到、并发改不同列、同列冲突、删除与更新（并发/支配/复活）、
唯一键/非空/类型约束进入 quarantine、节点重放与输入顺序无关性。
