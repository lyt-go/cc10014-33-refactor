"""转换编排 DAG 拓扑接口：返回节点、连线图结构。"""
from __future__ import annotations

import csv
import io
import json
from typing import Any

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import StreamingResponse

from app.schemas import ActionPayload, ActionResult, EntryPayload, PageResult
from app.services.transform_dag import TransformDagService

router = APIRouter(prefix="/api/transform_dag", tags=["转换编排"])
service = TransformDagService()
LIST_FIELDS = ["节点编号", "输入表名", "输出表名", "转换脚本", "执行顺序", "依赖节点", "失败策略", "调度周期"]
STATUSES = ["草稿", "待校验", "已生效", "已停用"]


def csv_cell(value: Any) -> str:
    text = str(value) if value is not None else ""
    if text and text[0] in "=+-@":
        text = "'" + text
    return '"' + text.replace('"', '""') + '"'


@router.get("")
def get_graph() -> dict[str, Any]:
    """返回节点与边组成的 DAG 图数据。"""
    return service.get_graph()


@router.get("/export")
def export_entries() -> StreamingResponse:
    nodes, edges = service.raw_data()
    buf = io.StringIO(newline="")
    writer = csv.writer(buf)
    writer.writerow(["type", "id", "name", "from", "to", "label", "status"])
    for node in nodes:
        writer.writerow(["node", node.get("id",""), csv_cell(node.get("name","")), "", "", "", node.get("status","")])
    for edge in edges:
        writer.writerow(["edge", "", "", edge.get("from",""), edge.get("to",""), edge.get("label",""), ""])
    return StreamingResponse(iter([buf.getvalue()]), media_type="text/csv; charset=utf-8",
                             headers={"Content-Disposition": "attachment; filename=transform_dag.csv"})


@router.get("/{entry_id}")
def get_entry(entry_id: int) -> dict:
    entry = service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"转换节点 {entry_id} 不存在")
    return entry


@router.post("")
def create_entry(payload: EntryPayload) -> ActionResult:
    entry_or_none, missing = service.create_entry(payload.values)
    if missing:
        return ActionResult(ok=False, message=f"缺少必填字段：{'、'.join(missing)}")
    return ActionResult(ok=True, message="转换节点已登记", entry=entry_or_none)


@router.post("/{entry_id}/actions")
def run_action(entry_id: int, payload: ActionPayload) -> ActionResult:
    entry_or_none, message = service.run_action(entry_id, payload.action)
    if entry_or_none is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=entry_or_none)


@router.post("/{entry_id}/execute")
def execute_node(entry_id: int) -> dict[str, Any]:
    """执行转换节点：消费上游表数据，写入下游表，自动生成血缘。"""
    result = service.execute_node(entry_id)
    return result


@router.get("/outputs/datasets")
def list_output_datasets() -> dict[str, Any]:
    """列出转换节点输出数据集及其单向状态机状态。"""
    items = service.list_output_datasets()
    return {"items": items, "total": len(items)}


@router.get("/outputs/{output_table}")
def get_output_dataset(output_table: str) -> dict[str, Any]:
    """输出数据集页面：原字段顺序数据、排序后字段视图与新旧度量对照。"""
    view = service.get_output_dataset_view(output_table)
    if not view.get("ok"):
        raise HTTPException(status_code=404, detail=view.get("message", "输出数据集不存在"))
    return view


@router.post("/outputs/{output_table}/advance")
def advance_output_status(output_table: str) -> ActionResult:
    """输出数据集状态单步推进；跳级或回退会被拒绝。"""
    entry, message = service.advance_output_status(output_table)
    if entry is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=entry)


@router.get("/outputs/{output_table}/conflict")
def get_output_conflict(output_table: str) -> dict[str, Any]:
    """比较同一输出数据集上多个转换来源的处理先后与优先级。"""
    from app.services import transform_pipeline
    return transform_pipeline.resolve_output_conflict(output_table)
