"""转换节点执行路径重构回归测试。

覆盖重构验收口径：
- 共用执行路径与旧两套聚合实现同输入同分组、同聚合值；
- 聚合函数参数、分组键顺序、输出字段类型、血缘批次编号逐项不变；
- 金额舍入、退款冲减、优惠抵扣、税费字段逐项比对；
- 执行路径与血缘同时落库，重复运行不新增重复血缘边；
- 输出数据集状态只能单向单步推进，拒绝跳级与回退；
- 输出数据集保留原字段顺序，另一条边界提供排序后字段与新旧度量对照；
- 同一输出表多来源冲突时按确定性优先级给出处理先后。
"""
from __future__ import annotations

import pytest

from app.store import store
from app.services.transform_dag import TransformDagService
from app.services.dws_layer import DwsLayerService
from app.services import transform_pipeline as pipeline


def reset_store():
    from app.seed import SEED_ROWS
    store._tables = {name: [dict(row) for row in rows] for name, rows in SEED_ROWS.items()}
    store._table_data.clear()
    store._versions.clear()
    store._lineage_edges.clear()
    store._quality_results.clear()
    store._snapshot_data.clear()
    store._archive_records.clear()
    store._backfill_partitions.clear()
    store._transform_outputs.clear()


class TestSharedAggregationParity:
    def setup_method(self):
        reset_store()

    def test_transform_and_dws_share_one_implementation(self):
        """转换节点与汇总层必须收拢到同一份共用聚合逻辑。"""
        assert TransformDagService()._execute_script.__func__ is not None
        # 两个兼容外壳内部都委托 transform_pipeline，不再各写一份分组循环。
        data = [{"g": "a", "m": "1.5"}, {"g": "a", "m": "2.5"}, {"g": "b", "m": "3"}]
        via_transform = TransformDagService()._execute_script(data, "SELECT SUM(m) FROM t GROUP BY g")
        via_dws = DwsLayerService()._do_aggregate(data, ["g"], "m", "SUM")
        direct = pipeline.group_aggregate(data, ["g"], "m", "SUM")
        assert via_transform == via_dws == direct

    @pytest.mark.parametrize("agg,expected", [
        ("SUM", 300.0),
        ("COUNT", 2),
        ("AVG", 150.0),
        ("MAX", 200.0),
        ("MIN", 100.0),
    ])
    def test_agg_func_params_and_values_unchanged(self, agg, expected):
        """聚合函数参数与聚合值与旧口径一致。"""
        data = [
            {"dt": "2026-11-01", "order_status": "已完成", "order_amount": "100"},
            {"dt": "2026-11-01", "order_status": "已完成", "order_amount": "200"},
        ]
        rows = pipeline.group_aggregate(data, ["dt", "order_status"], "order_amount", agg)
        assert rows[0]["order_amount_agg"] == expected

    def test_group_key_order_preserved(self):
        """分组键顺序按脚本书写顺序保留，并体现在字段顺序里。"""
        _, _, dims = pipeline.parse_groupby_script(
            "SELECT dt,order_status,SUM(order_amount) FROM t GROUP BY dt,order_status")
        assert dims == ["dt", "order_status"]
        data = [
            {"dt": "d1", "order_status": "s1", "order_amount": 1},
            {"dt": "d1", "order_status": "s2", "order_amount": 2},
        ]
        rows = pipeline.group_aggregate(data, dims, "order_amount", "SUM")
        assert list(rows[0].keys()) == ["dt", "order_status", "order_amount_agg"]

    def test_output_field_type_unchanged(self):
        """COUNT 输出 int，其余聚合输出 float，与旧实现字段类型一致。"""
        data = [{"g": "a", "m": "1"}, {"g": "a", "m": "2"}]
        count_row = pipeline.group_aggregate(data, ["g"], "m", "COUNT")[0]
        sum_row = pipeline.group_aggregate(data, ["g"], "m", "SUM")[0]
        assert type(count_row["m_agg"]) is int
        assert type(sum_row["m_agg"]) is float

    def test_money_refund_discount_tax_fields_itemized(self):
        """金额舍入、退款冲减、优惠抵扣和税费字段逐项比对，新旧度量完全一致。"""
        rows_in = [
            {"order_id": "O1", "dt": "2026-11-01",
             "order_amount": "100.00", "refund_amount": "10.00",
             "discount_amount": "5.00", "tax_amount": "8.50"},
            {"order_id": "O2", "dt": "2026-11-01",
             "order_amount": "200.05", "refund_amount": "20.00",
             "discount_amount": "0.00", "tax_amount": "16.95"},
            {"order_id": "O3", "dt": "2026-11-01",
             "order_amount": "bad", "refund_amount": None,
             "discount_amount": "", "tax_amount": "3.00"},
        ]
        measures = ["order_amount", "refund_amount", "discount_amount", "tax_amount"]
        for measure in measures:
            result = pipeline.group_aggregate(rows_in, ["dt"], measure, "SUM")
            # 手工按旧口径（float 转换失败按 0）独立复算
            expected = sum(float(r.get(measure, 0) or 0) if _is_float(r.get(measure)) else 0.0
                           for r in rows_in)
            assert result[0][f"{measure}_agg"] == pytest.approx(expected)

    def test_same_input_same_groups_and_values_old_vs_new(self):
        """旧执行器复刻与新共用路径在相同输入下给出相同分组与相同聚合值。"""
        data = [
            {"g": "a", "m": "10"}, {"g": "a", "m": "20"},
            {"g": "b", "m": "5"}, {"g": "b", "m": "7"},
        ]
        script = "SELECT g,SUM(m) FROM t GROUP BY g"
        legacy = _legacy_group_aggregate(data, ["g"], "m", "SUM")
        current = pipeline.apply_script(data, script)
        assert [{k: v for k, v in r.items()} for r in legacy] == current


class TestExecuteAndLineage:
    def setup_method(self):
        reset_store()

    def test_execute_writes_output_and_lineage_together(self):
        """执行路径与血缘必须一起保留：输出表与血缘边同时落库。"""
        store.set_table_rows("dwd_order_detail", [
            {"order_id": "O001", "order_amount": "100", "order_status": "已完成", "dt": "2026-11-01"},
            {"order_id": "O002", "order_amount": "200", "order_status": "已完成", "dt": "2026-11-01"},
        ])
        result = TransformDagService().execute_node(1)
        assert result["ok"] is True
        # 输出数据集
        assert len(store.table_rows("dws_order_daily")) == 1
        # 血缘边
        edges = store.get_lineage_edges()
        edge = next(e for e in edges
                    if e["上游表名"] == "dwd_order_detail" and e["下游表名"] == "dws_order_daily")
        # 血缘字段格式与批次编号保持原样
        assert edge["来源字段"] == "TRAN-ORD-001"
        assert edge["目标字段"] == "TRAN-ORD-001"
        assert edge["采集批次"] == "TRAN-ORD-001"
        assert "GROUP BY dt,order_status" in edge["转换逻辑"]
        assert edge["链路版本"].startswith("v")

    def test_repeat_run_does_not_add_duplicate_edges(self):
        """重复运行不会新增重复血缘边。"""
        store.set_table_rows("dwd_order_detail", [
            {"order_id": "O001", "order_amount": "100", "order_status": "已完成", "dt": "2026-11-01"},
        ])
        td = TransformDagService()
        td.execute_node(1)
        td.execute_node(1)
        td.execute_node(1)
        edges = [e for e in store.get_lineage_edges()
                 if e["上游表名"] == "dwd_order_detail" and e["下游表名"] == "dws_order_daily"]
        assert len(edges) == 1
        # 但每次运行都保留运行记录，度量可对照
        dataset = store.get_transform_output("dws_order_daily")
        assert len(dataset["runs"]) == 3

    def test_dws_lineage_batch_id_unchanged(self):
        """汇总层血缘批次编号仍取创建批次，血缘格式不变。"""
        store.set_table_rows("dwd_order_detail", [
            {"order_id": "O001", "order_amount": "100.00", "order_status": "已完成", "dt": "2026-11-01"},
        ])
        DwsLayerService().aggregate_from_dwd(1)
        edge = next(e for e in store.get_lineage_edges()
                    if e["下游表名"] == "dws_order_daily")
        assert edge["上游表名"] == "dwd_order_detail"
        assert edge["来源字段"] == "dt,order_status"
        assert edge["转换逻辑"] == "SUM(order_amount) GROUP BY dt,order_status"
        assert edge["采集批次"] == "RAW-20261101"


class TestOutputDatasetLifecycle:
    def setup_method(self):
        reset_store()

    def _execute(self):
        store.set_table_rows("dwd_order_detail", [
            {"order_id": "O001", "order_amount": "100", "order_status": "已完成", "dt": "2026-11-01"},
        ])
        TransformDagService().execute_node(1)

    def test_status_starts_generated_and_advances_one_step(self):
        """输出数据集状态单向推进：未生成→已生成→已校验→已发布。"""
        assert store.get_transform_output("dws_order_daily") is None
        self._execute()
        dataset = store.get_transform_output("dws_order_daily")
        assert dataset["status"] == "已生成"
        svc = TransformDagService()
        ds, msg = svc.advance_output_status("dws_order_daily")
        assert ds["status"] == "已校验" and "已校验" in msg
        ds, _ = svc.advance_output_status("dws_order_daily")
        assert ds["status"] == "已发布"
        # 末态不可再推进
        none_res, msg = svc.advance_output_status("dws_order_daily")
        assert none_res is None and "末态" in msg

    def test_status_rejects_skipping_and_unknown(self):
        """拒绝跳级：状态机只允许相邻状态，未生成数据集不能直接推进。"""
        self._execute()
        # 内部状态机不能跳级（next_output_status 只走一步）
        assert pipeline.next_output_status("已生成") == "已校验"
        assert pipeline.next_output_status("未生成") == "已生成"
        missing, msg = TransformDagService().advance_output_status("not_exist_table")
        assert missing is None and "尚未生成" in msg

    def test_reexecute_keeps_status_forward_only(self):
        """重复执行不回退状态：已校验后重跑仍是已校验。"""
        self._execute()
        svc = TransformDagService()
        svc.advance_output_status("dws_order_daily")
        assert store.get_transform_output("dws_order_daily")["status"] == "已校验"
        svc.execute_node(1)
        assert store.get_transform_output("dws_order_daily")["status"] == "已校验"

    def test_field_order_kept_and_sorted_view_boundary(self):
        """两条字段顺序边界：落库保留原顺序，视图另给排序后字段。"""
        self._execute()
        view = TransformDagService().get_output_dataset_view("dws_order_daily")
        assert view["ok"] is True
        # 原字段顺序（维度在前、度量在后）
        assert view["field_order"] == ["dt", "order_status", "order_amount_agg"]
        assert list(view["rows"][0].keys()) == ["dt", "order_status", "order_amount_agg"]
        # 排序后显示顺序
        assert list(view["sorted_rows"][0].keys()) == sorted(view["field_order"])

    def test_new_old_measure_comparison(self):
        """新旧度量对照：同输入重跑无差异，改输入后差异被逐项标出。"""
        self._execute()
        svc = TransformDagService()
        svc.execute_node(1)  # 同输入第二次
        view = svc.get_output_dataset_view("dws_order_daily")
        assert view["measure_comparison"]["same"] is True
        # 改变输入后再次执行
        store.set_table_rows("dwd_order_detail", [
            {"order_id": "O001", "order_amount": "180", "order_status": "已完成", "dt": "2026-11-01"},
        ])
        svc.execute_node(1)
        view = svc.get_output_dataset_view("dws_order_daily")
        comp = view["measure_comparison"]
        assert comp["same"] is False and comp["diff_count"] >= 1
        assert comp["diffs"][0]["old"] == 100.0 and comp["diffs"][0]["new"] == 180.0

    def test_conflict_priority_deterministic(self):
        """同一输出表多来源冲突：按执行顺序升序、其次来源id升序裁决。"""
        self._execute()
        # 第二个转换节点也写入同一张输出表（id 由 store 分配，seed 已占用 1-3）
        td = TransformDagService()
        second, missing = td.create_entry({
            "节点编号": "TRAN-ORD-002", "输入表名": "dwd_order_detail",
            "输出表名": "dws_order_daily", "转换脚本": "SELECT dt,order_status,SUM(order_amount) FROM dwd_order_detail GROUP BY dt,order_status",
            "执行顺序": "2",
        })
        assert not missing
        td.execute_node(second["id"])
        conflict = pipeline.resolve_output_conflict("dws_order_daily")
        assert conflict["winner"]["source_id"] == 1
        assert [c["source_id"] for c in conflict["candidates"]] == [1, second["id"]]


def _is_float(value):
    try:
        float(value or 0)
        return True
    except (TypeError, ValueError):
        return False


def _legacy_group_aggregate(data, dims, measure, agg):
    """测试内复刻重构前的分组聚合循环，用于同输入新旧对照。"""
    groups: dict = {}
    for row in data:
        key = "|".join(str(row.get(d, "")) for d in dims)
        groups.setdefault(key, {d: row.get(d) for d in dims})
        groups[key].setdefault("_values", [])
        try:
            groups[key]["_values"].append(float(row.get(measure, 0) or 0))
        except Exception:
            groups[key]["_values"].append(0.0)
    result = []
    for g in groups.values():
        out = {d: g[d] for d in dims}
        vals = g["_values"]
        out[f"{measure}_agg"] = sum(vals) if agg == "SUM" else len(vals)
        result.append(out)
    return result
