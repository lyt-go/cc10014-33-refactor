"""转换节点共用执行路径：取数 → 分组聚合 → 输出数据集落库 → 血缘落库的唯一实现。

重构背景：转换节点（transform_dag）与汇总层（dws_layer）原先各写了一份
GROUP BY 聚合实现，重复修改容易造成口径分叉。本模块把分组键解析、聚合函数、
输出字段命名、血缘落库收拢为一份共用逻辑，两条执行链路都只做各自的取数与
参数装配，实际加工统一走这里。

兼容性约束（任何一项变化都视为转换回归）：
- 聚合函数参数（SUM/COUNT/AVG/MAX/MIN 及其度量字段）保持原样；
- 分组键顺序保持原脚本/配置中的书写顺序；
- 输出字段名仍为 ``{measure}_agg``，字段类型与旧实现一致（int / float）；
- 输出行内字段顺序保持「先维度后度量」的原顺序，不做排序；
- 血缘字段（上游表、下游表、来源/目标字段、转换逻辑、采集批次）保持原样。
"""
from __future__ import annotations

import re
from datetime import datetime
from typing import Any

from app.store import store

SUPPORTED_AGG = ("SUM", "COUNT", "AVG", "MAX", "MIN")
# 输出数据集状态机：只允许沿该顺序单步向前推进，拒绝跳级与回退。
OUTPUT_STATUS_ORDER = ["未生成", "已生成", "已校验", "已发布"]

_AGG_PATTERN = re.compile(r"(SUM|COUNT|AVG|MAX|MIN)\s*\(([^)]+)\)", re.I)
_AGG_PREFIX_PATTERN = re.compile(r"^(SUM|COUNT|AVG|MAX|MIN)\s*\(", re.I)
# 与旧转换节点解析器保持一致的聚合关键字（含大小写两档）。
_AGG_KEYWORDS = [
    " SUM(", " COUNT(", " AVG(", " MAX(", " MIN(",
    " sum(", " count(", " avg(", " max(", " min(",
]


# ---------------------------------------------------------------------------
# 1. 脚本 / 配置解析
# ---------------------------------------------------------------------------

def parse_groupby_script(script: str) -> tuple[str, str, list[str]]:
    """解析 ``... GROUP BY ... AGG(measure) ...`` 风格的转换脚本。

    返回 ``(聚合函数, 度量字段, 分组维度列表)``，解析规则与旧
    ``TransformDagService._execute_script`` 完全一致（含大小写与截断行为），
    保证同一脚本得到同一组聚合参数。
    """
    agg_m = _AGG_PATTERN.search(script or "")
    measure = agg_m.group(2).strip() if agg_m else ""
    agg = agg_m.group(1).upper() if agg_m else "SUM"
    upper_script = (script or "").upper()
    if "GROUP BY" in upper_script:
        after_gb = script.split("GROUP BY", 1)[1].strip()
    else:
        after_gb = script or ""
    # 去掉 GROUP BY 片段末尾的聚合函数部分，只保留维度列表。
    for kw in _AGG_KEYWORDS:
        if kw in after_gb.upper():
            idx = after_gb.upper().index(kw)
            after_gb = after_gb[:idx]
            break
    dims_raw = [d.strip() for d in after_gb.split(",")]
    dims = [d for d in dims_raw if d and not _AGG_PREFIX_PATTERN.search(d)]
    return agg, measure, dims


def is_groupby_script(script: str) -> bool:
    return "GROUP BY" in (script or "").upper()


# ---------------------------------------------------------------------------
# 2. 共用聚合实现（旧 transform_dag._execute_script 与 dws_layer._do_aggregate 的合并体）
# ---------------------------------------------------------------------------

def _to_float(value: Any) -> float:
    """旧实现的度量取值口径：无法转成数字时按 0.0 处理。"""
    try:
        return float(value or 0)
    except Exception:
        return 0.0


def _apply_agg(agg: str, values: list[float], measure: str) -> Any:
    """按旧口径计算单一聚合值；输出字段名仍由调用方拼成 ``{measure}_agg``。"""
    if agg == "SUM":
        return sum(values)
    if agg == "COUNT":
        # 旧实现 COUNT 返回 int，保持输出字段类型不变。
        return len(values)
    if agg == "AVG":
        return sum(values) / len(values) if values else 0
    if agg == "MAX":
        return max(values) if values else 0
    if agg == "MIN":
        return min(values) if values else 0
    # 未知聚合函数按 SUM 兜底，与旧实现一致。
    return sum(values)


def group_aggregate(
    data: list[dict[str, Any]],
    dims: list[str],
    measure: str,
    agg: str,
) -> list[dict[str, Any]]:
    """按 ``dims`` 顺序分组并对 ``measure`` 执行 ``agg`` 聚合。

    分组首次出现的顺序、行内「维度字段在前、``{measure}_agg`` 在后」的字段
    顺序均与旧两套实现保持一致，因此同输入必然得到同分组、同聚合值。
    """
    groups: dict[str, dict[str, Any]] = {}
    for row in data:
        key = "|".join(str(row.get(d, "")) for d in dims) if dims else "*"
        if key not in groups:
            groups[key] = {d: row.get(d) for d in dims} if dims else {}
            groups[key]["_values"] = []
        groups[key]["_values"].append(_to_float(row.get(measure, 0)))
    result: list[dict[str, Any]] = []
    for g in groups.values():
        out = {d: g[d] for d in dims} if dims else {}
        out[f"{measure}_agg"] = _apply_agg(agg, g["_values"], measure)
        result.append(out)
    return result


def apply_script(data: list[dict[str, Any]], script: str) -> list[dict[str, Any]]:
    """转换脚本执行入口：含 GROUP BY 走聚合，否则按旧口径整表透传。"""
    if is_groupby_script(script):
        agg, measure, dims = parse_groupby_script(script)
        return group_aggregate(data, dims, measure, agg)
    return [dict(r) for r in data]


# ---------------------------------------------------------------------------
# 3. 血缘落库（幂等：重复执行不新增重复边）
# ---------------------------------------------------------------------------

def _lineage_signature(upstream: str, downstream: str, field_map: str,
                       transform_logic: str, batch_id: str) -> tuple[str, ...]:
    return (upstream, downstream, field_map or "", transform_logic or "", batch_id or "")


def record_lineage(upstream: str, downstream: str, field_map: str,
                   transform_logic: str = "", batch_id: str = "") -> dict[str, Any]:
    """写入血缘边；同口径的边已存在时直接复用，保证重复运行不产生重复边。

    血缘字段与 ``Store.add_lineage`` 的历史格式逐项一致，仅在落库前做去重。
    """
    signature = _lineage_signature(upstream, downstream, field_map, transform_logic, batch_id)
    for edge in store.get_lineage_edges():
        existed = _lineage_signature(
            edge.get("上游表名", ""), edge.get("下游表名", ""),
            edge.get("来源字段", ""), edge.get("转换逻辑", ""), edge.get("采集批次", ""),
        )
        if existed == signature:
            return edge
    return store.add_lineage(upstream, downstream, field_map,
                             transform_logic=transform_logic, batch_id=batch_id)


# ---------------------------------------------------------------------------
# 4. 输出数据集落库 + 状态机 + 冲突优先级
# ---------------------------------------------------------------------------

def _derive_output_schema(output_table: str) -> None:
    """血缘落库后推导 schema；与旧执行路径行为一致，无上游 schema 时为空操作。"""
    from app.services.schema_registry import SchemaRegistryService
    SchemaRegistryService().derive_schema_from_lineage(output_table)


def register_output_dataset(
    *,
    output_table: str,
    source_kind: str,
    source_id: int,
    source_code: str,
    input_table: str,
    rows: list[dict[str, Any]],
    agg: str,
    measure: str,
    dims: list[str],
    script: str,
    batch_id: str,
    lineage_edge: dict[str, Any],
    execution_order: str = "",
) -> dict[str, Any]:
    """登记一次转换输出数据集快照，并把状态单向推进到「已生成」。

    每次执行都会留一条运行记录（含度量快照与字段顺序），用于新旧度量对照与
    冲突比较；重复执行不会回退状态，只沿状态机向前。
    """
    field_order = list(rows[0].keys()) if rows else [*dims, f"{measure}_agg"] if measure else list(dims)
    metric_snapshot = [
        {"group_key": "|".join(str(r.get(d, "")) for d in dims) if dims else "*",
         "measure_field": f"{measure}_agg",
         "value": r.get(f"{measure}_agg") if measure else None}
        for r in rows
    ]
    record = {
        "run_id": 0,
        "source_kind": source_kind,
        "source_id": source_id,
        "source_code": source_code,
        "input_table": input_table,
        "output_table": output_table,
        "agg_func": agg,
        "measure": measure,
        "group_keys": list(dims),
        "field_order": field_order,
        "script": script,
        "batch_id": batch_id,
        "lineage_id": lineage_edge.get("id"),
        "lineage_version": lineage_edge.get("链路版本", ""),
        "row_count": len(rows),
        "metric_snapshot": metric_snapshot,
        "execution_order": str(execution_order or ""),
        "ran_at": datetime.now().isoformat(),
    }
    return store.save_transform_output(output_table, record)


def next_output_status(current: str) -> str | None:
    """返回状态机中的下一状态；已到末态或状态未知时返回 None。"""
    try:
        idx = OUTPUT_STATUS_ORDER.index(current)
    except ValueError:
        return None
    if idx + 1 >= len(OUTPUT_STATUS_ORDER):
        return None
    return OUTPUT_STATUS_ORDER[idx + 1]


def advance_output_status(output_table: str) -> tuple[dict[str, Any] | None, str]:
    """输出数据集状态单步推进；跳级、回退与未生成的数据集一律拒绝。"""
    dataset = store.get_transform_output(output_table)
    if dataset is None:
        return None, f"输出数据集 {output_table} 尚未生成，不能推进状态"
    current = dataset.get("status", OUTPUT_STATUS_ORDER[0])
    target = next_output_status(current)
    if target is None:
        return None, f"输出数据集 {output_table} 已处于末态「{current}」，无可推进状态"
    dataset["status"] = target
    dataset["updated_at"] = datetime.now().isoformat()
    return dataset, f"输出数据集 {output_table} 状态已推进为「{target}」"


def conflict_priority_key(record: dict[str, Any]) -> tuple[int, int]:
    """同一输出表被多个来源写入时的确定性优先级。

    口径（当前尚无业务结论，先固化为确定性规则，集中在此便于后续调整）：
    执行顺序（``执行顺序``，按数值比较，非数字排最后）升序，其次来源 id 升序，
    即编排中越靠前、编号越小的转换节点优先。
    """
    raw_order = str(record.get("execution_order", "")).strip()
    try:
        order = int(float(raw_order))
        numeric = 0
    except (TypeError, ValueError):
        order = 0
        numeric = 1
    return (numeric, order) if numeric else (0, order), int(record.get("source_id", 0))


def resolve_output_conflict(output_table: str) -> dict[str, Any]:
    """比较同一输出数据集上各次处理的先后，给出优先来源与全部候选。"""
    dataset = store.get_transform_output(output_table)
    if dataset is None:
        return {"output_table": output_table, "winner": None, "candidates": []}
    runs = list(dataset.get("runs", []))
    winner = None
    if runs:
        winner = min(runs, key=conflict_priority_key)
    return {
        "output_table": output_table,
        "status": dataset.get("status"),
        "priority_rule": "执行顺序升序，其次来源节点id升序",
        "winner": winner,
        "candidates": sorted(runs, key=conflict_priority_key),
    }


# ---------------------------------------------------------------------------
# 5. 字段顺序的两条边界：原顺序（兼容历史数据集）与排序后顺序（对照视图）
# ---------------------------------------------------------------------------

def sorted_field_view(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """边界视图：按字段名排序后的输出，仅用于页面对照，不改变落库原顺序。"""
    if not rows:
        return []
    columns = sorted({key for row in rows for key in row.keys()})
    return [{col: row.get(col) for col in columns} for row in rows]


def compare_measure_snapshots(previous: list[dict[str, Any]],
                              current: list[dict[str, Any]]) -> dict[str, Any]:
    """新旧两次执行的度量逐项对照（旧执行器 vs 新共用执行路径同输入口径）。"""
    prev_map = {item.get("group_key"): item.get("value") for item in (previous or [])}
    cur_map = {item.get("group_key"): item.get("value") for item in (current or [])}
    diffs: list[dict[str, Any]] = []
    for key in sorted(set(prev_map) | set(cur_map)):
        old_value = prev_map.get(key)
        new_value = cur_map.get(key)
        if old_value != new_value:
            diffs.append({"group_key": key, "old": old_value, "new": new_value})
    return {
        "same": not diffs,
        "diff_count": len(diffs),
        "diffs": diffs,
        "old_groups": len(prev_map),
        "new_groups": len(cur_map),
    }


# ---------------------------------------------------------------------------
# 6. 共用执行路径：取数装配之后的唯一加工主流程
# ---------------------------------------------------------------------------

def run_aggregation_pipeline(
    *,
    input_table: str,
    output_table: str,
    input_data: list[dict[str, Any]],
    dims: list[str],
    measure: str,
    agg: str,
    script: str,
    field_map: str,
    batch_id: str,
    source_kind: str,
    source_id: int,
    source_code: str,
    execution_order: str = "",
) -> dict[str, Any]:
    """转换节点与汇总层共用的执行主路径。

    步骤固定为：聚合加工 → 输出数据集落库（保留原字段顺序）→ 血缘落库
    （执行路径与血缘必须一起保留）→ schema 推导 → 输出数据集登记与状态推进。
    """
    output_data = group_aggregate(input_data, dims, measure, agg)
    store.set_table_rows(output_table, output_data)
    lineage_edge = record_lineage(
        input_table, output_table, field_map,
        transform_logic=script, batch_id=batch_id,
    )
    _derive_output_schema(output_table)
    dataset = register_output_dataset(
        output_table=output_table,
        source_kind=source_kind,
        source_id=source_id,
        source_code=source_code,
        input_table=input_table,
        rows=output_data,
        agg=agg,
        measure=measure,
        dims=dims,
        script=script,
        batch_id=batch_id,
        lineage_edge=lineage_edge,
        execution_order=execution_order,
    )
    return {
        "rows": output_data,
        "lineage": lineage_edge,
        "dataset": dataset,
    }
