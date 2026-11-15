"""接收编排：拉取信封、冲刷确认队列、按界补收。

恢复边界约定：

- 写入：每条信封一个事务，崩溃只影响未提交的那一条；
- 确认：只在数据提交后发送；网络失败留在队列下次再发，网关明确拒绝记为失败；
- 补收：每个断档每次运行最多尝试一次，累计尝试有上限；未决的断档不阻塞
  其他序号与其他站点的写入和确认。
"""

import json
import tempfile
from pathlib import Path
from urllib.parse import quote

from .client import Client
from .decoding import records
from .origin import serve
from .subscriptions import DEFAULT_REPUBLISH_ATTEMPTS, Receiver


def fetch_envelopes(client, feed_path="/feed"):
    response = client.fetch(feed_path)
    if response.status != 200:
        raise RuntimeError(f"feed returned status {response.status}")
    return records(response.body, response.headers.get("content-type", ""))


def load_envelopes(path):
    """从本地 JSON 文件读取信封列表（对象数组或 {"items": [...]}）。"""
    data = json.loads(Path(path).read_text(encoding="utf-8"))
    items = data.get("items") if isinstance(data, dict) else data
    if not isinstance(items, list) or any(not isinstance(item, dict) for item in items):
        raise ValueError("envelope file must hold a list of objects")
    return items


def flush_acks(receiver, client):
    """把已持久化的确认按入队顺序发往网关；返回本次成功发送的数量。"""
    sent = 0
    for ack in receiver.queued_acks():
        try:
            response = client.post_form("/acks", {
                "gateway": ack["gateway"],
                "generation": ack["generation"],
                "sequence": ack["sequence"],
            })
        except OSError as error:
            receiver.record_event("ack_send_failed", {"ack": ack["id"], "error": type(error).__name__})
            continue
        if 200 <= response.status < 300:
            receiver.mark_acks_sent([ack["id"]])
            sent += 1
        else:
            receiver.mark_ack_failed(ack["id"], f"gateway status {response.status}")
    return sent


def republish_pending(receiver, client, max_attempts=DEFAULT_REPUBLISH_ATTEMPTS):
    """对待补收断档逐个请求 Republish；每个断档每次运行最多尝试一次。"""
    outcome = {"attempted": 0, "filled": [], "permanent": [], "failed": []}
    for gap in receiver.pending_gaps(max_attempts):
        outcome["attempted"] += 1
        key = {"gateway": gap["gateway"], "generation": gap["generation"], "sequence": gap["sequence"]}
        path = "/republish/" + quote(gap["generation"], safe="") + "/" + str(gap["sequence"])
        try:
            response = client.fetch(path)
        except OSError as error:
            receiver.record_republish_error(**key, note=type(error).__name__)
            outcome["failed"].append(gap["sequence"])
            continue
        if response.status != 200:
            receiver.record_republish_error(**key, note=f"status {response.status}")
            outcome["failed"].append(gap["sequence"])
            continue
        try:
            payload = json.loads(response.body)
        except json.JSONDecodeError:
            payload = None
        status = payload.get("status") if isinstance(payload, dict) else None
        if isinstance(status, str) and status.startswith("Bad"):
            receiver.mark_gap_permanent(**key, note=status)
            outcome["permanent"].append(gap["sequence"])
            continue
        try:
            if not isinstance(payload, dict) or (
                payload.get("gateway"),
                payload.get("subscription_generation"),
                payload.get("sequence"),
            ) != (gap["gateway"], gap["generation"], gap["sequence"]):
                raise ValueError("republished envelope does not match the requested gap")
            receiver.ingest(payload)
        except ValueError as error:
            receiver.record_event("republish_mismatch", {**key, "error": str(error)})
            receiver.record_republish_error(**key, note="mismatch")
            outcome["failed"].append(gap["sequence"])
            continue
        outcome["filled"].append(gap["sequence"])
    return outcome


def subscribe_once(receiver, client=None, feed_path="/feed", envelope_file=None,
                   max_attempts=DEFAULT_REPUBLISH_ATTEMPTS):
    """完成一轮接收：取信封、逐条落库、冲刷确认、按界补收、再次冲刷确认。"""
    if envelope_file is not None:
        envelopes = load_envelopes(envelope_file)
    elif client is not None:
        envelopes = fetch_envelopes(client, feed_path)
    else:
        raise ValueError("client or envelope_file is required")
    summary = {
        "received": 0, "duplicates": 0, "observations": 0, "keepalives": 0,
        "new_gaps": [], "recovered": [], "rejected": [],
    }
    for index, raw in enumerate(envelopes):
        try:
            result = receiver.ingest(raw)
        except ValueError as error:
            summary["rejected"].append({"index": index, "error": str(error)})
            continue
        summary["received"] += 1
        summary["duplicates"] += int(result.duplicate)
        summary["observations"] += result.observations
        summary["keepalives"] += int(result.keepalive and not result.duplicate)
        for sequence in result.new_gaps:
            summary["new_gaps"].append({
                "gateway": result.gateway, "generation": result.generation, "sequence": sequence,
            })
        if result.recovered:
            summary["recovered"].append({
                "gateway": result.gateway, "generation": result.generation, "sequence": result.sequence,
            })
    summary["acks_sent"] = flush_acks(receiver, client) if client is not None else 0
    if client is not None:
        summary["republish"] = republish_pending(receiver, client, max_attempts)
        summary["acks_sent"] += flush_acks(receiver, client)
    else:
        summary["republish"] = {"attempted": 0, "filled": [], "permanent": [], "failed": []}
    summary["acks_queued"] = len(receiver.queued_acks())
    return summary


def drill():
    """本地演练：空目录启动本地网关，完整走一遍接收、补收、确认与重放。

    只使用本地网关记录，不连接或控制真实设备；结束自动清理。
    """
    with tempfile.TemporaryDirectory() as temporary, serve() as origin:
        receiver = Receiver(Path(temporary) / "reception.sqlite3")
        try:
            client = Client(origin.base_url)
            first = subscribe_once(receiver, client)
            replay = subscribe_once(receiver, client)  # 网关重发整批：不得重复形成观测
            return {
                "first_pass": first,
                "replay_pass": replay,
                "stations": [receiver.station_view(gateway) for gateway in receiver.stations()],
                "gateway_ack_ledger": [dict(entry) for entry in origin.acks],
                "events": receiver.events(),
            }
        finally:
            receiver.close()
