"""订阅通知的可靠接收：先写后确认、断档登记与补收边界。

网关把 OPC UA SDK 的 Publish/Republish 响应解成 JSON 信封。本模块负责：

- 校验信封，并持久化订阅代际、监控项绑定以及每条值的质量与源时间；
- 先写后确认：确认与数据落库在同一事务排入本地可审计队列，提交成功后才可发往网关；
- 序号按 (站点, 代际) 独立跟踪，是非零 32 位计数并回绕到 1；保活占用序号但不产生观测；
- 序号断档登记为待补收；服务器把报文移出重传队列（Republish 返回 Bad 类状态）后转为
  永久缺口，不用当前值填平历史；
- 监控项绑定按代际与序号生效：观测按“不超过其序号的最近一次绑定”解析传感器，
  绑定更新与在途通知交错时，旧句柄不会被解释成新传感器；无法解析的句柄保持未解析。
"""

import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

SEQ_MAX = 0xFFFFFFFF  # 序号为非零 32 位计数，回绕到 1
MAX_GAP_SPAN = 4096  # 单次可逐条登记的断档长度上限，超出只记事件不物化
MAX_SEQUENCE_JUMP = 1 << 20  # 新序号与已收窗口的最大合理距离，防止异常序号污染状态
DEFAULT_REPUBLISH_ATTEMPTS = 3  # 每个断档的补收尝试上限，不无限等待缺失报文

SCHEMA = """
CREATE TABLE IF NOT EXISTS subscriptions (
    gateway TEXT NOT NULL,
    generation TEXT NOT NULL,
    subscription_id INTEGER,
    first_seen TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    last_seen TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    last_available TEXT,
    PRIMARY KEY (gateway, generation)
);
CREATE TABLE IF NOT EXISTS messages (
    gateway TEXT NOT NULL,
    generation TEXT NOT NULL,
    sequence INTEGER NOT NULL,
    keepalive INTEGER NOT NULL,
    subscription_id INTEGER,
    available TEXT,
    received_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    PRIMARY KEY (gateway, generation, sequence)
);
CREATE TABLE IF NOT EXISTS binding_events (
    gateway TEXT NOT NULL,
    generation TEXT NOT NULL,
    sequence INTEGER NOT NULL,
    handle INTEGER NOT NULL,
    sensor TEXT NOT NULL,
    PRIMARY KEY (gateway, generation, sequence, handle)
);
CREATE INDEX IF NOT EXISTS binding_events_lookup
    ON binding_events (gateway, generation, handle, sequence);
CREATE TABLE IF NOT EXISTS observations (
    gateway TEXT NOT NULL,
    generation TEXT NOT NULL,
    sequence INTEGER NOT NULL,
    handle INTEGER NOT NULL,
    value TEXT NOT NULL,
    quality TEXT NOT NULL,
    source_time TEXT NOT NULL,
    received_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    PRIMARY KEY (gateway, generation, sequence, handle)
);
CREATE TABLE IF NOT EXISTS gaps (
    gateway TEXT NOT NULL,
    generation TEXT NOT NULL,
    sequence INTEGER NOT NULL,
    state TEXT NOT NULL DEFAULT 'pending',
    attempts INTEGER NOT NULL DEFAULT 0,
    note TEXT,
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    PRIMARY KEY (gateway, generation, sequence)
);
CREATE TABLE IF NOT EXISTS ack_queue (
    id INTEGER PRIMARY KEY,
    gateway TEXT NOT NULL,
    generation TEXT NOT NULL,
    sequence INTEGER NOT NULL,
    state TEXT NOT NULL DEFAULT 'queued',
    queued_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    sent_at TEXT,
    UNIQUE (gateway, generation, sequence)
);
CREATE TABLE IF NOT EXISTS events (
    id INTEGER PRIMARY KEY,
    kind TEXT NOT NULL,
    detail TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);
"""

NOW = "strftime('%Y-%m-%dT%H:%M:%fZ','now')"


def seq_next(sequence):
    """序号的下一个值；非零 32 位计数回绕到 1。"""
    return 1 if sequence >= SEQ_MAX else sequence + 1


def seq_forward_distance(start, end):
    """从 start 到 end 沿递增方向（回绕跳过 0）的步数；相同为 0。"""
    if start == end:
        return 0
    if end > start:
        return end - start
    return (SEQ_MAX - start) + end


@dataclass(frozen=True)
class Notification:
    handle: int
    value: object
    quality: str
    source_time: str


@dataclass(frozen=True)
class Envelope:
    gateway: str
    generation: str
    sequence: int
    keepalive: bool
    subscription_id: int | None
    available: tuple | None  # None 表示信封未携带该字段
    bindings: dict
    notifications: tuple


@dataclass(frozen=True)
class IngestResult:
    gateway: str
    generation: str
    sequence: int
    duplicate: bool
    keepalive: bool
    observations: int
    new_gaps: tuple
    recovered: bool  # 本消息填补了此前登记的断档


def _text(value, name):
    if not isinstance(value, str) or not value.strip():
        raise ValueError(name + " must be a non-empty string")
    return value.strip()


def _required_text(raw, name):
    if name not in raw:
        raise ValueError("missing field: " + name)
    return _text(raw[name], name)


def _sequence(value):
    if isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= SEQ_MAX:
        raise ValueError("sequence must be a non-zero 32-bit integer")
    return value


def _handle(value):
    # 通知中的句柄是整数；bindings 的键经 JSON 对象序列化后为数字字符串，统一归一为整数。
    if isinstance(value, bool):
        raise ValueError("handle must be a 32-bit integer")
    if isinstance(value, str):
        if not value.isdigit():
            raise ValueError("handle must be a 32-bit integer")
        value = int(value)
    if not isinstance(value, int) or not 0 <= value <= SEQ_MAX:
        raise ValueError("handle must be a 32-bit integer")
    return value


def parse_envelope(raw):
    """校验并规范化一条网关信封；不合法时抛出 ValueError，不写入任何状态。"""
    if not isinstance(raw, dict):
        raise ValueError("envelope must be a JSON object")
    gateway = _required_text(raw, "gateway")
    generation = _required_text(raw, "subscription_generation")
    if "sequence" not in raw:
        raise ValueError("missing field: sequence")
    sequence = _sequence(raw["sequence"])
    keepalive = raw.get("keepalive", False)
    if not isinstance(keepalive, bool):
        raise ValueError("keepalive must be a boolean")
    subscription_id = raw.get("subscription_id")
    if subscription_id is not None and (
        isinstance(subscription_id, bool)
        or not isinstance(subscription_id, int)
        or not 0 <= subscription_id <= SEQ_MAX
    ):
        raise ValueError("subscription_id must be a 32-bit integer")
    available = raw.get("available")
    if available is not None:
        if not isinstance(available, list):
            raise ValueError("available must be a list of sequence numbers")
        available = tuple(_sequence(item) for item in available)
    bindings = {}
    raw_bindings = raw.get("bindings")
    if raw_bindings is not None:
        if not isinstance(raw_bindings, dict):
            raise ValueError("bindings must be an object mapping handle to sensor")
        for key, sensor in raw_bindings.items():
            bindings[_handle(key)] = _text(sensor, "binding sensor")
    raw_notifications = raw.get("notifications", [])
    if not isinstance(raw_notifications, list):
        raise ValueError("notifications must be a list")
    notifications = []
    for item in raw_notifications:
        if not isinstance(item, dict):
            raise ValueError("notification must be an object")
        if "handle" not in item:
            raise ValueError("notification missing handle")
        if "value" not in item:
            raise ValueError("notification missing value")
        quality = _text(item.get("quality"), "quality")
        source_time = _text(item.get("source_time"), "source_time")
        try:
            datetime.fromisoformat(source_time)
        except ValueError:
            raise ValueError("invalid source_time: " + source_time) from None
        notifications.append(Notification(_handle(item["handle"]), item["value"], quality, source_time))
    if keepalive and notifications:
        raise ValueError("keepalive carries no data notifications")
    handles = [item.handle for item in notifications]
    if len(set(handles)) != len(handles):
        raise ValueError("duplicate handle within one message")
    return Envelope(
        gateway, generation, sequence, keepalive, subscription_id, available, bindings, tuple(notifications)
    )


def _encode_available(available):
    return None if available is None else json.dumps(list(available))


class Receiver:
    """可靠接收状态库：每个信封一个事务，确认先入队、提交后再发送。"""

    def __init__(self, path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.connection = sqlite3.connect(self.path, timeout=5)
        self.connection.row_factory = sqlite3.Row
        self.connection.executescript(SCHEMA)

    def close(self):
        self.connection.close()

    def _event(self, connection, kind, detail):
        connection.execute(
            "INSERT INTO events(kind,detail) VALUES (?,?)",
            (kind, json.dumps(detail, ensure_ascii=False, sort_keys=True, default=str)),
        )

    def record_event(self, kind, detail):
        with self.connection:
            self._event(self.connection, kind, detail)

    def events(self, kind=None):
        query = "SELECT * FROM events"
        params = ()
        if kind is not None:
            query += " WHERE kind=?"
            params = (kind,)
        rows = self.connection.execute(query + " ORDER BY id", params).fetchall()
        return [{**dict(row), "detail": json.loads(row["detail"])} for row in rows]

    def ingest(self, raw):
        """接收一条信封：持久化消息、绑定与观测，登记断档，并把确认排入队列。

        重复投递按 (站点, 代际, 序号) 去重，不重复形成观测；内容不一致的重发记入事件。
        序号与已收窗口距离过远的信封拒绝落库并记事件，防止异常序号污染状态。
        """
        envelope = parse_envelope(raw)
        connection = self.connection
        key = (envelope.gateway, envelope.generation, envelope.sequence)
        result = None
        with connection:
            if connection.execute(
                "SELECT 1 FROM messages WHERE gateway=? AND generation=? AND sequence=?", key
            ).fetchone():
                # 重复投递：元数据已在首次投递时落库，这里只核对内容并确保确认在队。
                self._check_redelivery(connection, envelope)
                self._queue_ack(connection, envelope)
                result = IngestResult(*key, True, envelope.keepalive, 0, (), False)
            else:
                sequences = [
                    row[0]
                    for row in connection.execute(
                        "SELECT sequence FROM messages WHERE gateway=? AND generation=?", key[:2]
                    )
                ]
                too_far = sequences and min(
                    min(seq_forward_distance(seen, envelope.sequence), seq_forward_distance(envelope.sequence, seen))
                    for seen in sequences
                ) > MAX_SEQUENCE_JUMP
                if too_far:
                    self._event(connection, "sequence_jump_rejected", {
                        "gateway": envelope.gateway,
                        "generation": envelope.generation,
                        "sequence": envelope.sequence,
                    })
                else:
                    self._track_subscription(connection, envelope)
                    connection.execute(
                        "INSERT INTO messages(gateway,generation,sequence,keepalive,subscription_id,available)"
                        " VALUES (?,?,?,?,?,?)",
                        (*key, int(envelope.keepalive), envelope.subscription_id, _encode_available(envelope.available)),
                    )
                    for handle, sensor in envelope.bindings.items():
                        connection.execute(
                            "INSERT OR IGNORE INTO binding_events VALUES (?,?,?,?,?)",
                            (*key[:2], envelope.sequence, handle, sensor),
                        )
                    for item in envelope.notifications:
                        connection.execute(
                            "INSERT OR IGNORE INTO observations"
                            " (gateway,generation,sequence,handle,value,quality,source_time)"
                            " VALUES (?,?,?,?,?,?,?)",
                            (
                                *key,
                                item.handle,
                                json.dumps(item.value, ensure_ascii=False, sort_keys=True),
                                item.quality,
                                item.source_time,
                            ),
                        )
                    recovered = connection.execute(
                        "SELECT state FROM gaps WHERE gateway=? AND generation=? AND sequence=?", key
                    ).fetchone()
                    if recovered is not None:
                        self._event(connection, "gap_recovered", {
                            "gateway": envelope.gateway,
                            "generation": envelope.generation,
                            "sequence": envelope.sequence,
                            "previous_state": recovered["state"],
                        })
                    new_gaps = self._refresh_gaps(connection, *key[:2])
                    self._queue_ack(connection, envelope)
                    result = IngestResult(
                        *key, False, envelope.keepalive, len(envelope.notifications), tuple(new_gaps),
                        recovered is not None,
                    )
        if result is None:
            raise ValueError(
                f"sequence {envelope.sequence} too far from the received window of "
                f"{envelope.gateway}/{envelope.generation}"
            )
        return result

    def _track_subscription(self, connection, envelope):
        """登记或刷新订阅代际；available 是服务器重传队列快照，只随新消息前进。"""
        key = (envelope.gateway, envelope.generation)
        subscription = connection.execute(
            "SELECT subscription_id FROM subscriptions WHERE gateway=? AND generation=?", key
        ).fetchone()
        if subscription is None:
            connection.execute(
                "INSERT INTO subscriptions(gateway,generation,subscription_id,last_available) VALUES (?,?,?,?)",
                (*key, envelope.subscription_id, _encode_available(envelope.available)),
            )
            return
        if (
            envelope.subscription_id is not None
            and subscription["subscription_id"] is not None
            and subscription["subscription_id"] != envelope.subscription_id
        ):
            self._event(connection, "subscription_id_conflict", {
                "gateway": envelope.gateway,
                "generation": envelope.generation,
                "stored": subscription["subscription_id"],
                "received": envelope.subscription_id,
            })
        if envelope.available is not None:
            connection.execute(
                f"UPDATE subscriptions SET last_seen={NOW}, last_available=?,"
                " subscription_id=COALESCE(subscription_id,?) WHERE gateway=? AND generation=?",
                (_encode_available(envelope.available), envelope.subscription_id, *key),
            )
        else:
            connection.execute(
                f"UPDATE subscriptions SET last_seen={NOW},"
                " subscription_id=COALESCE(subscription_id,?) WHERE gateway=? AND generation=?",
                (envelope.subscription_id, *key),
            )

    def _check_redelivery(self, connection, envelope):
        """重发内容应与首次落库一致；不一致只记事件，保留首次内容。"""
        key = (envelope.gateway, envelope.generation, envelope.sequence)
        problems = []
        row = connection.execute(
            "SELECT keepalive FROM messages WHERE gateway=? AND generation=? AND sequence=?", key
        ).fetchone()
        if bool(row["keepalive"]) != envelope.keepalive:
            problems.append("keepalive")
        stored = connection.execute(
            "SELECT handle,value,quality,source_time FROM observations"
            " WHERE gateway=? AND generation=? AND sequence=? ORDER BY handle",
            key,
        ).fetchall()
        current = sorted(envelope.notifications, key=lambda item: item.handle)
        if [(r["handle"], r["value"], r["quality"], r["source_time"]) for r in stored] != [
            (n.handle, json.dumps(n.value, ensure_ascii=False, sort_keys=True), n.quality, n.source_time)
            for n in current
        ]:
            problems.append("notifications")
        stored_bindings = {
            r["handle"]: r["sensor"]
            for r in connection.execute(
                "SELECT handle,sensor FROM binding_events WHERE gateway=? AND generation=? AND sequence=?", key
            )
        }
        if stored_bindings != envelope.bindings:
            problems.append("bindings")
        if problems:
            self._event(connection, "conflicting_redelivery", {
                "gateway": envelope.gateway,
                "generation": envelope.generation,
                "sequence": envelope.sequence,
                "fields": problems,
            })

    def _queue_ack(self, connection, envelope):
        # 确认与数据在同一事务入队；只有事务提交后才允许发往网关。
        connection.execute(
            "INSERT OR IGNORE INTO ack_queue(gateway,generation,sequence) VALUES (?,?,?)",
            (envelope.gateway, envelope.generation, envelope.sequence),
        )

    def _refresh_gaps(self, connection, gateway, generation):
        """按已收消息集合重算断档：最大前向间隔视为未到达的尾部，其余间隔为缺口。"""
        sequences = sorted(
            row[0]
            for row in connection.execute(
                "SELECT sequence FROM messages WHERE gateway=? AND generation=?", (gateway, generation)
            )
        )
        missing = set()
        count = len(sequences)
        if count > 1:
            pairs = [(sequences[i], sequences[(i + 1) % count]) for i in range(count)]
            tail = max(range(count), key=lambda i: seq_forward_distance(*pairs[i]))
            for i, (start, end) in enumerate(pairs):
                if i == tail:
                    continue
                span = seq_forward_distance(start, end) - 1
                if span <= 0:
                    continue
                if span > MAX_GAP_SPAN:
                    self._event(connection, "gap_span_overflow", {
                        "gateway": gateway,
                        "generation": generation,
                        "after": start,
                        "before": end,
                        "span": span,
                    })
                    continue
                current = start
                for _ in range(span):
                    current = seq_next(current)
                    missing.add(current)
        existing = {
            row["sequence"]
            for row in connection.execute(
                "SELECT sequence FROM gaps WHERE gateway=? AND generation=?", (gateway, generation)
            )
        }
        for sequence in existing - missing:
            connection.execute(
                "DELETE FROM gaps WHERE gateway=? AND generation=? AND sequence=?",
                (gateway, generation, sequence),
            )
        created = []
        for sequence in sorted(missing - existing):
            connection.execute(
                "INSERT INTO gaps(gateway,generation,sequence) VALUES (?,?,?)",
                (gateway, generation, sequence),
            )
            created.append(sequence)
        return created

    def queued_acks(self):
        rows = self.connection.execute(
            "SELECT * FROM ack_queue WHERE state='queued' ORDER BY id"
        ).fetchall()
        return [dict(row) for row in rows]

    def mark_acks_sent(self, ack_ids):
        with self.connection:
            for ack_id in ack_ids:
                self.connection.execute(
                    f"UPDATE ack_queue SET state='sent', sent_at={NOW} WHERE id=? AND state='queued'",
                    (ack_id,),
                )

    def mark_ack_failed(self, ack_id, detail):
        with self.connection:
            self.connection.execute(
                "UPDATE ack_queue SET state='failed' WHERE id=? AND state='queued'", (ack_id,)
            )
            self._event(self.connection, "ack_rejected", {"ack": ack_id, "detail": detail})

    def pending_gaps(self, max_attempts=DEFAULT_REPUBLISH_ATTEMPTS):
        """仍可补收的断档：未被判为永久，且补收尝试未达上限。"""
        rows = self.connection.execute(
            "SELECT * FROM gaps WHERE state='pending' AND attempts<? ORDER BY gateway,generation,sequence",
            (max_attempts,),
        ).fetchall()
        return [dict(row) for row in rows]

    def mark_gap_permanent(self, gateway, generation, sequence, note):
        """服务器已把报文移出重传队列：登记为永久缺口，不造历史值。"""
        with self.connection:
            cursor = self.connection.execute(
                f"UPDATE gaps SET state='permanent', attempts=attempts+1, note=?, updated_at={NOW}"
                " WHERE gateway=? AND generation=? AND sequence=? AND state='pending'",
                (note, gateway, generation, sequence),
            )
            if cursor.rowcount:
                self._event(self.connection, "gap_permanent", {
                    "gateway": gateway, "generation": generation, "sequence": sequence, "note": note,
                })
            return cursor.rowcount > 0

    def record_republish_error(self, gateway, generation, sequence, note):
        """补收尝试失败（非最终结论）：计入尝试次数，断档保持待补收。"""
        with self.connection:
            cursor = self.connection.execute(
                f"UPDATE gaps SET attempts=attempts+1, note=?, updated_at={NOW}"
                " WHERE gateway=? AND generation=? AND sequence=? AND state='pending'",
                (note, gateway, generation, sequence),
            )
            if cursor.rowcount:
                self._event(self.connection, "republish_error", {
                    "gateway": gateway, "generation": generation, "sequence": sequence, "note": note,
                })
            return cursor.rowcount > 0

    def stations(self):
        rows = self.connection.execute("SELECT DISTINCT gateway FROM subscriptions ORDER BY gateway").fetchall()
        return [row[0] for row in rows]

    def station_view(self, gateway):
        """值班员视图：已收观测（含质量与源时间）、仍可补收序号、永久缺口。"""
        generations = []
        subscriptions = self.connection.execute(
            "SELECT * FROM subscriptions WHERE gateway=? ORDER BY first_seen,generation", (gateway,)
        ).fetchall()
        for subscription in subscriptions:
            key = (gateway, subscription["generation"])
            messages = self.connection.execute(
                "SELECT sequence,keepalive FROM messages WHERE gateway=? AND generation=? ORDER BY sequence", key
            ).fetchall()
            observations = self.connection.execute(
                "SELECT o.sequence,o.handle,o.value,o.quality,o.source_time,"
                " (SELECT b.sensor FROM binding_events b"
                "  WHERE b.gateway=o.gateway AND b.generation=o.generation AND b.handle=o.handle"
                "    AND b.sequence<=o.sequence ORDER BY b.sequence DESC LIMIT 1) AS sensor"
                " FROM observations o WHERE o.gateway=? AND o.generation=? ORDER BY o.sequence,o.handle",
                key,
            ).fetchall()
            bindings = self.connection.execute(
                "SELECT handle,sensor FROM binding_events b"
                " WHERE gateway=? AND generation=? AND sequence=("
                "   SELECT MAX(sequence) FROM binding_events"
                "   WHERE gateway=b.gateway AND generation=b.generation AND handle=b.handle)"
                " ORDER BY handle",
                key,
            ).fetchall()
            pending = self.connection.execute(
                "SELECT sequence,attempts,note FROM gaps"
                " WHERE gateway=? AND generation=? AND state='pending' ORDER BY sequence",
                key,
            ).fetchall()
            permanent = self.connection.execute(
                "SELECT sequence,note FROM gaps"
                " WHERE gateway=? AND generation=? AND state='permanent' ORDER BY sequence",
                key,
            ).fetchall()
            generations.append({
                "generation": subscription["generation"],
                "subscription_id": subscription["subscription_id"],
                "received_sequences": [row["sequence"] for row in messages],
                "keepalive_sequences": [row["sequence"] for row in messages if row["keepalive"]],
                "observations": [
                    {
                        "sequence": row["sequence"],
                        "handle": row["handle"],
                        "sensor": row["sensor"],
                        "resolved": row["sensor"] is not None,
                        "value": json.loads(row["value"]),
                        "quality": row["quality"],
                        "source_time": row["source_time"],
                    }
                    for row in observations
                ],
                "bindings": {str(row["handle"]): row["sensor"] for row in bindings},
                "recoverable_sequences": [
                    {"sequence": row["sequence"], "republish_attempts": row["attempts"], "note": row["note"]}
                    for row in pending
                ],
                "permanent_gaps": [
                    {"sequence": row["sequence"], "note": row["note"]} for row in permanent
                ],
                "retransmission_available": (
                    json.loads(subscription["last_available"])
                    if subscription["last_available"] is not None
                    else None
                ),
            })
        acknowledgements = {"sent": 0, "queued": 0, "failed": 0}
        for row in self.connection.execute(
            "SELECT state,COUNT(*) AS count FROM ack_queue WHERE gateway=? GROUP BY state", (gateway,)
        ):
            acknowledgements[row["state"]] = row["count"]
        return {"gateway": gateway, "generations": generations, "acknowledgements": acknowledgements}
