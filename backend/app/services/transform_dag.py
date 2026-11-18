"""转换编排 DAG 图服务：支持节点执行时消费上游表数据，写入下游表，并自动生成血缘。

重构说明：节点执行不再自带聚合实现，分组聚合、输出数据集落库与血缘落库统一
走 ``app.services.transform_pipeline`` 共用执行路径；本服务只负责取数装配与
DAG 图维护。保留 ``_execute_script`` 作为兼容外壳，内部委托共用逻辑。
"""
from __future__ import annotations

from typing import Any

from app.store import store
from app.services import transform_pipeline as pipeline

MODULE = "transform_dag"
REQUIRED_FIELDS = ["节点编号", "输入表名", "输出表名"]
STATUS_ORDER = ["草稿", "待校验", "已生效", "已停用"]
ACTION_RULES = {"提交校验": "待校验", "标记生效": "已生效", "停用节点": "已停用"}
NEGATIVE_ACTIONS = ["停用节点"]


def csv_cell(value: Any) -> str:
    text = str(value) if value is not None else ""
    if text and text[0] in "=+-@":
        text = "'" + text
    return '"' + text.replace('"', '""') + '"'


class TransformDagService:
    def get_graph(self) -> dict[str, Any]:
        rows = store.rows(MODULE)
        nodes: list[dict] = []
        edges: list[dict] = []
        for i, row in enumerate(rows):
            nodes.append({"id": int(row.get("id", i + 1)),
                          "name": str(row.get("节点编号", "")),
                          "status": str(row.get("status", STATUS_ORDER[0])),
                          **{k: row[k] for k in row if k not in ("id", "status", "pending", "abnormal")}})
        # 基于依赖节点构建真实边
        name_to_id = {n["name"]: n["id"] for n in nodes}
        for node in nodes:
            dep = str(node.get("依赖节点", ""))
            if dep and dep in name_to_id:
                edges.append({"from": name_to_id[dep], "to": node["id"], "label": "依赖"})
        # 补充基于输入输出表的血缘边
        for i, a in enumerate(nodes):
            for b in nodes[i + 1:]:
                if a.get("输出表名") and a["输出表名"] == b.get("输入表名"):
                    edges.append({"from": a["id"], "to": b["id"], "label": "表依赖"})
        return {"nodes": nodes, "edges": edges}

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        return store.find(MODULE, entry_id)

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        missing = [f for f in REQUIRED_FIELDS if not str(values.get(f) or "").strip()]
        if missing:
            return None, missing
        rows = store.rows(MODULE)
        entry = {"id": max((int(r.get("id", 0)) for r in rows), default=0) + 1}
        entry.update({f: values.get(f) for f in REQUIRED_FIELDS})
        entry["status"] = STATUS_ORDER[0]
        entry["pending"] = True
        entry["abnormal"] = False
        for k in ["转换脚本", "执行顺序", "依赖节点", "失败策略", "调度周期"]:
            if k in values:
                entry[k] = values[k]
        rows.append(entry)
        return entry, []

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, f"转换节点 {entry_id} 不存在或已归档"
        if action not in ACTION_RULES:
            return None, f"动作「{action}」不属于转换编排可执行范围"
        target = ACTION_RULES[action]
        entry["status"] = target
        entry["pending"] = target != STATUS_ORDER[-1]
        entry["abnormal"] = action in NEGATIVE_ACTIONS
        store.save_version(MODULE, entry_id, entry)
        return entry, f"转换节点已{action}"

    def raw_data(self) -> tuple[list[dict], list[dict]]:
        g = self.get_graph()
        return g["nodes"], g["edges"]

    # ---- 转换节点输出数据集（状态机 / 对照视图） ----

    def list_output_datasets(self) -> list[dict[str, Any]]:
        return store.list_transform_outputs()

    def get_output_dataset(self, output_table: str) -> dict[str, Any] | None:
        return store.get_transform_output(output_table)

    def advance_output_status(self, output_table: str) -> tuple[dict[str, Any] | None, str]:
        """输出数据集状态只能单向单步推进，拒绝跳级。"""
        return pipeline.advance_output_status(output_table)

    def get_output_dataset_view(self, output_table: str) -> dict[str, Any]:
        """输出数据集页面对照视图。

        两条字段顺序边界同时返回：
        - ``rows``：落库的原字段顺序（兼容既有输出数据集，聚合值不变）；
        - ``sorted_rows``：按字段名排序后的显示顺序，附带新旧度量对照。
        """
        dataset = store.get_transform_output(output_table)
        if dataset is None:
            return {"ok": False, "message": f"输出数据集 {output_table} 尚未生成"}
        rows = store.table_rows(output_table)
        runs = dataset.get("runs", [])
        latest = runs[-1] if runs else None
        previous = runs[-2] if len(runs) >= 2 else None
        comparison = pipeline.compare_measure_snapshots(
            previous.get("metric_snapshot", []) if previous else [],
            latest.get("metric_snapshot", []) if latest else [],
        )
        conflict = pipeline.resolve_output_conflict(output_table)
        return {
            "ok": True,
            "output_table": output_table,
            "status": dataset.get("status"),
            "field_order": latest.get("field_order", []) if latest else [],
            "rows": rows,
            "sorted_rows": pipeline.sorted_field_view(rows),
            "measure_comparison": comparison,
            "conflict": conflict,
            "runs": runs,
        }

    # ---- 节点执行：取数 → 共用执行路径（聚合 + 输出落库 + 血缘落库） ----

    def execute_node(self, entry_id: int) -> dict[str, Any]:
        """执行转换节点：读取输入表数据，经共用执行路径加工，写入输出表，并自动生成血缘和推导schema。"""
        entry = self.get_entry(entry_id)
        if not entry:
            return {"ok": False, "message": "转换节点不存在"}
        input_table = entry.get("输入表名", "")
        output_table = entry.get("输出表名", "")
        script = entry.get("转换脚本", "")
        # 读取输入数据
        input_data = store.table_rows(input_table)
        if not input_data:
            from app.services.dwd_layer import DwdLayerService
            dwd = DwdLayerService()
            # 尝试构建DWD数据
            dwd_entry = store.find_by_field("dwd_layer", "表名称", input_table)
            if dwd_entry:
                dwd.build_dwd_from_ods(dwd_entry["id"])
            input_data = store.table_rows(input_table)
        if not input_data:
            return {"ok": False, "message": f"输入表 {input_table} 无数据"}

        node_code = entry.get("节点编号", "")
        if pipeline.is_groupby_script(script):
            agg, measure, dims = pipeline.parse_groupby_script(script)
            result = pipeline.run_aggregation_pipeline(
                input_table=input_table,
                output_table=output_table,
                input_data=input_data,
                dims=dims,
                measure=measure,
                agg=agg,
                script=script,
                field_map=node_code,
                batch_id=node_code,
                source_kind=MODULE,
                source_id=entry_id,
                source_code=node_code,
                execution_order=entry.get("执行顺序", ""),
            )
            output_data = result["rows"]
        else:
            # 非 GROUP BY 脚本：沿用旧的整表透传口径，但血缘与输出登记仍走共用路径。
            output_data = pipeline.apply_script(input_data, script)
            store.set_table_rows(output_table, output_data)
            lineage_edge = pipeline.record_lineage(
                input_table, output_table, node_code,
                transform_logic=script, batch_id=node_code,
            )
            from app.services.schema_registry import SchemaRegistryService
            SchemaRegistryService().derive_schema_from_lineage(output_table)
            pipeline.register_output_dataset(
                output_table=output_table,
                source_kind=MODULE,
                source_id=entry_id,
                source_code=node_code,
                input_table=input_table,
                rows=output_data,
                agg="",
                measure="",
                dims=[],
                script=script,
                batch_id=node_code,
                lineage_edge=lineage_edge,
                execution_order=entry.get("执行顺序", ""),
            )
        return {"ok": True, "message": f"节点执行完成，输出{len(output_data)}行到 {output_table}", "rows": output_data}

    def _execute_script(self, data: list[dict[str, Any]], script: str) -> list[dict[str, Any]]:
        """兼容外壳：旧调用方仍可使用，内部已收拢到共用执行路径。"""
        return pipeline.apply_script(data, script)
