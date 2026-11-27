"""本地消息入口：经核验身份之上的订阅、消息接续与接班。

领域事实来自 ``inputs/`` 四份合成资料：

- 设备目录给出已验证的证书持有事实，实例与代次决定身份，
  自报的 client_name 不能单独认领会话；
- 值班目录按接收实例分配订阅组、数据类别（主题）与班次范围；
- 消息记录携带发送实例、产生时的租户/产线归属与代次；
- 交付记录按连接、实例、组和办理引用（attempt）解释。

本模块不模拟真实设备网络，所有判断都在本机内存中完成，并通过
:mod:`mqtt_codec` 提供可实际收发的 MQTT 3.1.1 字节接口。
"""
from __future__ import annotations

import argparse
import json
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path

from mqtt_codec import (ConnectPacket, ProtocolError, build_publish, connack,
                       disconnect, parse_packet, pingresp, puback, suback)


class Observation(str, Enum):
    ACCEPTED = "accepted"
    SENT_UNKNOWN = "sent_unknown"
    ACK = "ack"


class HandoffPolicy(str, Enum):
    GROUP = "group"
    INSTANCE_PRIVATE = "instance-private"


class Redelivery(str, Enum):
    """未确认内容的三类处置。"""
    SAFE_REDELIVER = "safe-redeliver"        # 可安全重新投送
    VERIFY_ORIGINAL = "verify-original"      # 只能向原订阅者核对
    QUARANTINE = "quarantine"                # 必须隔离


class StopReason(str, Enum):
    SUBSCRIPTION_WITHDRAWN = "subscription-withdrawn"
    DEVICE_GENERATION = "device-generation"
    OUT_OF_RANGE = "out-of-range"
    TENANT_MISMATCH = "tenant-mismatch"
    NO_HANDOFF = "no-handoff"
    NOT_A_RECEIVER = "not-a-receiver"


class GatewayError(Exception):
    """网关领域错误基类。"""


class IdentityRejected(GatewayError):
    """证书持有事实未通过核验或设备未登记。"""


class PermissionDenied(GatewayError):
    """查看者不具备该产线/该组的管辖资格。"""


class LateAcknowledgment(GatewayError):
    """迟到确认所对应的办理引用已被新队列取代。"""


# MQTT 3.1.1 CONNACK 返回码
RC_ACCEPTED = 0
RC_IDENTIFIER_REJECTED = 2
RC_NOT_AUTHORIZED = 5


@dataclass(frozen=True)
class Identity:
    """经核验的连接身份：实例与代次才是身份本身。"""
    device_id: str
    client_name: str
    tenant: str
    production_line: str
    instance: str
    generation: int
    certificate_verified: bool

    @classmethod
    def from_device(cls, device_id, record):
        return cls(device_id, record["client_name"], record["tenant"],
                   record["production_line"], record["instance"],
                   record["generation"], bool(record.get("certificate_verified")))


@dataclass(frozen=True)
class SubscriptionRule:
    group: str
    instance: str
    tenant: str
    production_line: str
    range_start: int
    range_end: int
    topics: frozenset[str]
    handoff: bool


@dataclass(frozen=True)
class GroupSettings:
    group: str
    pending_handoff: str
    unconfirmed_instance_private: str
    qos: int


@dataclass(frozen=True)
class MessageRecord:
    message: str
    source: str
    generation: int
    tenant: str
    production_line: str
    topic: str
    category: str
    groups: tuple[str, ...]
    receiver: str | None
    handoff: HandoffPolicy | None
    at: int
    text: str


@dataclass
class Attempt:
    """一次办理引用：接班补投新开 attempt，同会话续传沿用原 attempt。"""
    attempt: str
    group: str
    instance: str
    connection_id: str
    packet_id: int
    observations: list[tuple[Observation, int]] = field(default_factory=list)

    @property
    def acked(self):
        return bool(self.observations) and self.observations[-1][0] is Observation.ACK


@dataclass
class GroupDelivery:
    message: str
    group: str
    attempts: list[Attempt] = field(default_factory=list)
    quarantined: bool = False
    stop_reason: StopReason | None = None

    @property
    def current(self):
        return self.attempts[-1] if self.attempts else None

    @property
    def state(self):
        if self.quarantined:
            return "quarantined"
        if not self.attempts:
            return "waiting"
        return "acknowledged" if self.current.acked else "pending"


@dataclass
class Session:
    """一个已登记应用实例的持久会话；多个实例可共用 client_name。"""
    session_id: str
    client_name: str
    owner: Identity
    subscriptions: dict[str, SubscriptionRule] = field(default_factory=dict)
    pending: set[tuple[str, str]] = field(default_factory=set)
    outbox: list[dict] = field(default_factory=list)
    ephemeral: bool = False
    closed: bool = False
    superseded_by: str | None = None


@dataclass
class Connection:
    connection_id: str
    identity: Identity
    session: Session
    clean_session: bool
    open: bool = True
    # packet_id -> (message, group, attempt_id)
    inflight: dict[int, tuple[str, str, str]] = field(default_factory=dict)
    outbox: list[bytes] = field(default_factory=list)
    next_packet_id: int = 1
    historical: bool = False
    gateway: object = None

    def feed(self, packet_bytes: bytes):
        """便捷方法：把一帧报文交给网关处理。"""
        return self.gateway.handle_frame(self, packet_bytes)


@dataclass(frozen=True)
class ConnectResult:
    connection_id: str
    identity: Identity
    session_present: bool
    clean_session: bool
    claimed: tuple[str, ...] = ()
    refused: tuple[dict, ...] = ()
    return_code: int = RC_ACCEPTED


@dataclass(frozen=True)
class PublishResult:
    message: str
    deliveries: dict[str, str]
    retained: bool


@dataclass(frozen=True)
class AckResult:
    message: str | None
    group: str | None
    attempt: str | None
    accepted: bool
    stale: bool = False


@dataclass(frozen=True)
class FrameReply:
    """一帧 MQTT 报文的处理结果：回送字节与可选的领域结果。"""
    response: bytes
    result: object | None = None


def _split_topic(topic):
    parts = topic.split("/")
    if len(parts) != 2 or not all(parts):
        raise GatewayError(f"主题 {topic!r} 不属于本演练的产线主题空间")
    return parts[0], parts[1]


class Gateway:
    """本地消息入口的领域引擎。"""

    def __init__(self, *, now=0):
        self.now = now
        self.devices: dict[str, Identity] = {}
        self.ownership_change: dict | None = None
        self.rules: dict[str, SubscriptionRule] = {}
        self.group_settings: dict[str, GroupSettings] = {}
        self.messages: dict[str, MessageRecord] = {}
        self.deliveries: dict[tuple[str, str], GroupDelivery] = {}
        self.sessions: dict[str, Session] = {}
        self.session_history: list[Session] = []
        self.connections: dict[str, Connection] = {}
        self.retained: dict[tuple[str, str, str], MessageRecord] = {}
        self.events: list[dict] = []
        self._message_seq = 0

    # ------------------------------------------------------------------ 装载
    @classmethod
    def from_source(cls, source, *, now=0):
        """source 可以是 Source 对象，也可以是 inputs 目录路径。"""
        if isinstance(source, (str, Path)):
            directory = Path(source)
            data = {path.stem: json.loads(path.read_text(encoding="utf-8"))
                    for path in directory.glob("*.json")}
        else:
            data = {name: source.records(name) for name in source.names()}
        return cls.from_records(data, now=now)

    @classmethod
    def from_records(cls, data, *, now=0):
        gateway = cls(now=now)
        for device_id, record in data.get("devices", {}).items():
            if "instance" in record:
                gateway.devices[device_id] = Identity.from_device(device_id, record)
            else:
                gateway.ownership_change = record
        for record in data.get("subscriptions", {}).values():
            if "instance" in record:
                lo, hi = record["range"]
                gateway.rules[record["instance"]] = SubscriptionRule(
                    record["group"], record["instance"], record["tenant"],
                    record["production_line"], lo, hi,
                    frozenset(record["topics"]), bool(record["handoff"]))
            else:
                gateway.group_settings[record["group"]] = GroupSettings(
                    record["group"], record["pending_handoff"],
                    record["unconfirmed_instance_private"], record["qos"])
        for record in data.get("messages", {}).values():
            line, category = _split_topic(record["topic"])
            groups = tuple(record.get("groups", ()))
            policy = HandoffPolicy(record["handoff"]) if "handoff" in record else None
            message = MessageRecord(
                record["message"], record["source"], record["generation"],
                record["tenant"], line, record["topic"], category, groups,
                record.get("receiver"), policy, record["at"], record["text"])
            gateway.messages[message.message] = message
            if category == "latest":
                gateway.retained[(message.topic, message.tenant,
                                  message.production_line)] = message
        gateway._seed_deliveries(data.get("deliveries", {}))
        gateway._seed_sessions()
        return gateway

    def _seed_deliveries(self, records):
        for record in records.values():
            key = (record["message"], record["group"])
            delivery = self.deliveries.setdefault(
                key, GroupDelivery(record["message"], record["group"]))
            attempt = next((item for item in delivery.attempts
                            if item.attempt == record["attempt"]), None)
            if attempt is None:
                attempt = Attempt(record["attempt"], record["group"],
                                  record["instance"], record["connection"],
                                  record["packet_id"])
                delivery.attempts.append(attempt)
            self._register_historical_connection(record)
            attempt.observations.append((Observation(record["observation"]),
                                         record["at"]))
        for delivery in self.deliveries.values():
            self._classify(delivery)

    def _register_historical_connection(self, record):
        connection_id = record["connection"]
        identity = self._identity_for_instance(record["instance"])
        session = self._session_for(identity, create=True)
        if connection_id not in self.connections:
            self.connections[connection_id] = Connection(
                connection_id, identity, session, False, open=False,
                historical=True)
        connection = self.connections[connection_id]
        connection.inflight[record["packet_id"]] = (
            record["message"], record["group"], record["attempt"])

    def _identity_for_instance(self, instance):
        for identity in self.devices.values():
            if identity.instance == instance:
                return identity
        raise GatewayError(f"交付记录引用了未登记实例 {instance}")

    def _seed_sessions(self):
        # 每个经核验设备都拥有按实例登记的持久会话，即使暂时没有订阅。
        for identity in self.devices.values():
            self._session_for(identity, create=True)
        # 每个值班实例把目录规则登记到自己的持久会话。
        for rule in self.rules.values():
            identity = self._identity_for_instance(rule.instance)
            session = self._session_for(identity, create=True)
            for category in rule.topics:
                session.subscriptions[category] = rule
        # 仍未确认的交付挂在当前办理实例的会话上。
        for (message_id, group), delivery in self.deliveries.items():
            if delivery.state != "pending":
                continue
            identity = self._identity_for_instance(delivery.current.instance)
            session = self._session_for(identity, create=True)
            session.pending.add((message_id, group))

    # ------------------------------------------------------------- 身份/规则
    def _session_id(self, identity):
        return f"{identity.client_name}@{identity.instance}"

    def _session_for(self, identity, *, create=False):
        session_id = self._session_id(identity)
        session = self.sessions.get(session_id)
        if session is None and create:
            session = Session(session_id, identity.client_name, identity)
            self.sessions[session_id] = session
        return session

    def _active_sessions_named(self, client_name):
        return [session for session in self.sessions.values()
                if session.client_name == client_name and not session.closed]

    def _all_sessions_named(self, client_name):
        return [session for session in self.sessions.values()
                if session.client_name == client_name] + \
               [session for session in self.session_history
                if session.client_name == client_name]

    def _rule_for(self, identity):
        return self.rules.get(identity.instance)

    def _emit(self, kind, **fields):
        event = {"event": kind, "at": self.now}
        event.update(fields)
        self.events.append(event)
        return event

    def _eligible(self, rule, message):
        """当前订阅规则是否覆盖这条消息的首次/重新交付。"""
        if rule.tenant != message.tenant or rule.production_line != message.production_line:
            return False
        if message.category not in rule.topics:
            return False
        if not (rule.range_start <= message.at <= rule.range_end):
            return False
        if message.groups and rule.group not in message.groups:
            return False
        if message.receiver and message.receiver != rule.instance:
            return False
        return True

    def _classify(self, delivery):
        message = self.messages[delivery.message]
        if delivery.quarantined:
            return Redelivery.QUARANTINE
        if delivery.state != "pending":
            return None
        if message.handoff is HandoffPolicy.INSTANCE_PRIVATE:
            # 值班目录约定：实例私有的未确认内容只能向原订阅者核对。
            return Redelivery.VERIFY_ORIGINAL
        return Redelivery.SAFE_REDELIVER

    # ------------------------------------------------------------- 连接入口
    def open_connection(self, device_id, *, clean_session=False, client_id=None,
                        connection_id=None):
        """以已核验的证书持有事实打开一条连接。"""
        identity = self.devices.get(device_id)
        if identity is None or not identity.certificate_verified:
            raise IdentityRejected(f"设备 {device_id} 未通过证书持有核验")
        if client_id is not None and client_id != identity.client_name:
            self._emit("connect-rejected", device=device_id,
                       reason="client-name-mismatch")
            return ConnectResult("", identity, False, clean_session,
                                 return_code=RC_IDENTIFIER_REJECTED)

        connection_id = connection_id or self._new_connection_id()
        own_session = self._session_for(identity, create=True)
        same_name = [session for session in self.sessions.values()
                     if session.client_name == identity.client_name
                     and session is not own_session
                     and (session.pending or session.outbox)]
        claimed, refused, session_present = (), (), False

        if clean_session:
            session = Session(f"ephemeral-{connection_id}", identity.client_name,
                              identity, ephemeral=True)
        else:
            session = own_session
            session.closed = False

        rule = self._rule_for(identity)
        if rule is not None and not session.ephemeral:
            for category in rule.topics:
                session.subscriptions.setdefault(category, rule)

        # 先登记连接，恢复与接班产生的报文才能直接进入该连接的 outbox。
        connection = Connection(connection_id, identity, session,
                                clean_session, gateway=self)
        self.connections[connection_id] = connection

        if not clean_session and own_session.pending:
            # 同一已登记实例恢复：只续传，不重新认领。
            session_present = True
            self._resume_session(connection_id, session)
        if not clean_session and same_name:
            # 别的实例留着同名会话：按接班资格逐条核对。
            claimed, refused = self._takeover(
                identity, same_name, session, connection)
            session_present = session_present or bool(claimed)

        self._emit("connect", connection=connection_id, device=device_id,
                   instance=identity.instance, generation=identity.generation,
                   clean=clean_session, session_present=session_present,
                   claimed=list(claimed), refused=list(refused))
        return ConnectResult(connection_id, identity, session_present,
                             clean_session, claimed, tuple(refused))

    def _new_connection_id(self):
        existing = {name for name in self.connections if name.startswith("c-")}
        index = len(self.connections) + 100
        while f"c-{index}" in existing:
            index += 1
        return f"c-{index}"

    def _resume_session(self, connection_id, session):
        """同实例恢复：未确认 PUBLISH 沿用原报文号并置 DUP。"""
        connection = self.connections.get(connection_id)
        for message_id, group in sorted(session.pending):
            attempt = self.deliveries[(message_id, group)].current
            attempt.observations.append((Observation.SENT_UNKNOWN, self.now))
            attempt.connection_id = connection_id
            if connection is not None:
                connection.inflight.setdefault(
                    attempt.packet_id, (message_id, group, attempt.attempt))
                message = self.messages[message_id]
                connection.outbox.append(build_publish(
                    attempt.packet_id, message.topic, message.text,
                    duplicate=True))
            self._emit("resend", connection=connection_id, message=message_id,
                       group=group, attempt=attempt.attempt,
                       packet_id=attempt.packet_id, duplicate=True)
        for item in list(session.outbox):
            if connection is not None:
                connection.outbox.append(build_publish(
                    item["packet_id"], item["topic"], item["payload"],
                    duplicate=True))
            self._emit("resend", connection=connection_id, message=item["message"],
                       packet_id=item["packet_id"], duplicate=True)

    def _takeover(self, new_owner, old_sessions, new_session, connection=None):
        rule = self._rule_for(new_owner)
        claimed, refused = [], []
        if rule is None:
            # 新实例没有值班登记（典型：设备代次更替），旧队列一律隔离。
            for old_session in old_sessions:
                for key in sorted(old_session.pending):
                    delivery = self.deliveries[key]
                    delivery.quarantined = True
                    delivery.stop_reason = StopReason.DEVICE_GENERATION
                    old_session.pending.discard(key)
                    refused.append({"key": list(key),
                                    "reason": StopReason.DEVICE_GENERATION.value})
                    self._emit("quarantine", message=key[0], group=key[1],
                               reason=StopReason.DEVICE_GENERATION.value)
                for item in old_session.outbox:
                    self._quarantine_message(item["message"],
                                             StopReason.DEVICE_GENERATION)
                old_session.outbox.clear()
                old_session.closed = True
                old_session.superseded_by = new_owner.instance
                self.session_history.append(old_session)
                self.sessions.pop(old_session.session_id, None)
            self._emit("handoff", new_instance=new_owner.instance,
                       claimed=claimed, refused=refused)
            return tuple(claimed), tuple(refused)
        for old_session in old_sessions:
            for key in sorted(old_session.pending):
                message_id, group = key
                delivery = self.deliveries[key]
                message = self.messages[message_id]
                reason = self._handoff_refusal(rule, new_owner, old_session,
                                               message, group)
                if reason is None:
                    self._open_new_attempt(delivery, rule, new_owner,
                                           connection)
                    new_session.pending.add(key)
                    old_session.pending.discard(key)
                    self._revoke_old_inflight(delivery.attempts[-2],
                                              message_id, group)
                    claimed.append(message_id)
                    continue
                if reason is StopReason.NO_HANDOFF and \
                        message.handoff is HandoffPolicy.INSTANCE_PRIVATE:
                    # 原部署实例私有：留给原订阅者核对，不隔离、不交接。
                    refused.append({"key": list(key),
                                    "reason": Redelivery.VERIFY_ORIGINAL.value})
                else:
                    delivery.quarantined = True
                    delivery.stop_reason = reason
                    old_session.pending.discard(key)
                    refused.append({"key": list(key), "reason": reason.value})
                    self._emit("quarantine", message=message_id, group=group,
                               reason=reason.value)
            # 接收端身份无权认领旧设备的发件队列，按代次改变隔离。
            for item in old_session.outbox:
                self._quarantine_message(item["message"],
                                         StopReason.DEVICE_GENERATION)
            old_session.outbox.clear()
            if not old_session.pending:
                old_session.closed = True
                old_session.superseded_by = new_owner.instance
                self.session_history.append(old_session)
                self.sessions.pop(old_session.session_id, None)
        self._emit("handoff", new_instance=new_owner.instance,
                   claimed=claimed, refused=refused)
        return tuple(claimed), tuple(refused)

    def _handoff_refusal(self, rule, new_owner, old_session, message, group):
        """按消息产生时归属、组交接范围与当前身份核对接班资格。"""
        old_rule = self.rules.get(old_session.owner.instance)
        if old_rule is None or not old_rule.handoff:
            return StopReason.NO_HANDOFF
        settings = self.group_settings.get(group)
        if settings is None or settings.pending_handoff != "group-only":
            return StopReason.NO_HANDOFF
        if message.handoff is HandoffPolicy.INSTANCE_PRIVATE:
            return StopReason.NO_HANDOFF
        if rule is None:
            return StopReason.NOT_A_RECEIVER
        # 换租户/换产线是身份层冲突，优先于组内交接开关。
        if new_owner.tenant != message.tenant or \
                new_owner.production_line != message.production_line:
            return StopReason.TENANT_MISMATCH
        if rule.group != group:
            return StopReason.NOT_A_RECEIVER
        if not rule.handoff:
            return StopReason.NO_HANDOFF
        # 交接范围按组的连续值班覆盖（前后班范围相接）核对，
        # 消息必须落在原订阅者的班次范围内，且组覆盖能衔接到当前班。
        if not (old_rule.range_start <= message.at <= old_rule.range_end):
            return StopReason.OUT_OF_RANGE
        if rule.range_start > old_rule.range_end + 1 or \
                rule.range_end < old_rule.range_start:
            return StopReason.OUT_OF_RANGE
        return None

    def _revoke_old_inflight(self, old_attempt, message_id, group):
        """新办理引用生效后，旧连接对该报文号的在途映射一律吊销。"""
        for connection in self.connections.values():
            mapping = connection.inflight.get(old_attempt.packet_id)
            if mapping == (message_id, group, old_attempt.attempt):
                del connection.inflight[old_attempt.packet_id]
                self._emit("inflight-revoked",
                           connection=connection.connection_id,
                           message=message_id, group=group,
                           packet_id=old_attempt.packet_id,
                           superseded_by=None)

    def _open_new_attempt(self, delivery, rule, owner, connection=None):
        attempt_id = f"try-{self._next_attempt_number()}"
        packet_id = self._allocate_packet_id(owner)
        connection_id = connection.connection_id if connection is not None \
            else f"pending-conn-{owner.instance}"
        attempt = Attempt(attempt_id, rule.group, owner.instance,
                          connection_id, packet_id)
        attempt.observations.append((Observation.SENT_UNKNOWN, self.now))
        delivery.attempts.append(attempt)
        if connection is not None:
            # 接班补投是一次新的业务交付：新办理引用、新报文号，置 DUP。
            connection.inflight[packet_id] = (
                delivery.message, rule.group, attempt_id)
            message = self.messages[delivery.message]
            connection.outbox.append(build_publish(
                packet_id, message.topic, message.text, duplicate=True))
        self._emit("handoff-delivery", message=delivery.message, group=delivery.group,
                   attempt=attempt_id, packet_id=packet_id, instance=owner.instance)

    def _next_attempt_number(self):
        numbers = [int(item.attempt.split("-", 1)[1])
                   for delivery in self.deliveries.values()
                   for item in delivery.attempts
                   if item.attempt.startswith("try-") and
                   item.attempt[4:].isdigit()]
        return max(numbers, default=0) + 1

    def _allocate_packet_id(self, identity):
        used = set()
        for connection in self.connections.values():
            if connection.identity.instance == identity.instance and connection.open:
                used.update(connection.inflight)
        candidate = 1
        while candidate in used:
            candidate += 1
        return candidate

    def close_connection(self, connection_id):
        connection = self.connections[connection_id]
        connection.open = False
        if connection.session.ephemeral:
            connection.session.closed = True
        self._emit("disconnect", connection=connection_id,
                   instance=connection.identity.instance)

    # ------------------------------------------------------------- 订阅入口
    def subscribe(self, connection, topic, qos=1):
        identity = connection.identity
        rule = self._rule_for(identity)
        line, category = _split_topic(topic)
        if rule is None:
            self._emit("subscribe-denied", connection=connection.connection_id,
                       topic=topic, reason=StopReason.NOT_A_RECEIVER.value)
            return False, StopReason.NOT_A_RECEIVER.value
        if line != identity.production_line or category not in rule.topics:
            self._emit("subscribe-denied", connection=connection.connection_id,
                       topic=topic, reason="out-of-catalog")
            return False, "out-of-catalog"
        connection.session.subscriptions[category] = rule
        self._emit("subscribe", connection=connection.connection_id, topic=topic,
                   group=rule.group, qos=qos)
        retained = self._retained_for(identity, topic)
        if retained is not None:
            packet_id = self._send_publish(connection, retained, attempt_meta=None)
            self._emit("retained-delivery", connection=connection.connection_id,
                       message=retained.message, packet_id=packet_id)
        self._deliver_waiting(connection, rule)
        return True, rule.group

    def _deliver_waiting(self, connection, rule):
        for (message_id, group), delivery in list(self.deliveries.items()):
            if group != rule.group or delivery.state != "waiting":
                continue
            message = self.messages[message_id]
            if not self._eligible(rule, message):
                continue
            self._attempt_delivery(delivery, message, connection, rule)

    def withdraw_subscription(self, instance, category, *, at=None):
        """值班目录撤回订阅；已交付记录原样保留。"""
        if at is not None:
            self.now = at
        rule = self.rules.get(instance)
        if rule is None:
            raise GatewayError(f"实例 {instance} 没有值班登记")
        for session in self._all_sessions_of(instance):
            session.subscriptions.pop(category, None)
        for (message_id, group), delivery in self.deliveries.items():
            message = self.messages[message_id]
            if message.category != category or group != rule.group:
                continue
            if delivery.state != "pending":
                continue
            if message.handoff is HandoffPolicy.INSTANCE_PRIVATE and \
                    delivery.current.instance == instance:
                # 实例私有且原订阅者退出：只能隔离，不参与组交接。
                delivery.quarantined = True
                delivery.stop_reason = StopReason.SUBSCRIPTION_WITHDRAWN
                for session in self._all_sessions_of(instance):
                    session.pending.discard((message_id, group))
                self._emit("quarantine", message=message_id, group=group,
                           reason=StopReason.SUBSCRIPTION_WITHDRAWN.value)
            # 组待办保留在原持久会话上，等待同组下一班接班；已交付记录不动。
        self._emit("withdraw", instance=instance, category=category)

    def _all_sessions_of(self, instance):
        return [session for session in self.sessions.values()
                if session.owner.instance == instance] + \
               [session for session in self.session_history
                if session.owner.instance == instance]

    # ------------------------------------------------------------- 发布入口
    def publish_device(self, connection, topic, payload, *, message_id=None,
                       groups=None, receiver=None, at=None, handoff=None):
        """已核验设备发送一条消息，返回各收件组的交付状态。"""
        if at is not None:
            self.now = at
        identity = connection.identity
        line, category = _split_topic(topic)
        if line != identity.production_line:
            raise PermissionDenied(f"设备只能发布本产线 {identity.production_line} 的主题")
        self._message_seq += 1
        message_id = message_id or f"live-{self._message_seq}"
        if groups is None and category == "alerts":
            groups = tuple(sorted({
                rule.group for rule in self.rules.values()
                if rule.tenant == identity.tenant
                and rule.production_line == identity.production_line
                and category in rule.topics
                and rule.range_start <= self.now <= rule.range_end}))
        policy = HandoffPolicy(handoff) if handoff else (
            HandoffPolicy.INSTANCE_PRIVATE if receiver else
            HandoffPolicy.GROUP if category == "alerts" else None)
        text = payload if isinstance(payload, str) else payload.decode("utf-8")
        message = MessageRecord(
            message_id, identity.instance, identity.generation, identity.tenant,
            line, topic, category, tuple(groups or ()), receiver, policy,
            self.now, text)
        self.messages[message_id] = message
        result_groups = {}
        retained = False
        if category == "latest":
            self.retained[(topic, identity.tenant, line)] = message
            retained = True
        else:
            for group in message.groups:
                delivery = GroupDelivery(message_id, group)
                self.deliveries[(message_id, group)] = delivery
                target = self._connected_rule(group, message)
                if target is None or (receiver and target[1].instance != receiver):
                    result_groups[group] = "waiting"
                    continue
                conn, rule = target
                self._attempt_delivery(delivery, message, conn, rule)
                result_groups[group] = delivery.state
        self._emit("publish", connection=connection.connection_id,
                   message=message_id, topic=topic, groups=list(message.groups),
                   retained=retained)
        return PublishResult(message_id, result_groups, retained)

    def _connected_rule(self, group, message):
        for connection in self.connections.values():
            if not connection.open:
                continue
            rule = self._rule_for(connection.identity)
            if rule and rule.group == group \
                    and message.category in connection.session.subscriptions \
                    and self._eligible(rule, message):
                return connection, rule
        return None

    def _attempt_delivery(self, delivery, message, connection, rule):
        attempt_id = f"try-{self._next_attempt_number()}"
        packet_id = self._take_packet_id(connection)
        attempt = Attempt(attempt_id, rule.group, connection.identity.instance,
                          connection.connection_id, packet_id)
        attempt.observations.append((Observation.SENT_UNKNOWN, self.now))
        delivery.attempts.append(attempt)
        connection.inflight[packet_id] = (message.message, rule.group, attempt_id)
        connection.session.pending.add((message.message, rule.group))
        connection.outbox.append(build_publish(packet_id, message.topic,
                                               message.text))
        self._emit("deliver", connection=connection.connection_id,
                   message=message.message, group=rule.group,
                   attempt=attempt_id, packet_id=packet_id)

    def _take_packet_id(self, connection):
        while connection.next_packet_id in connection.inflight:
            connection.next_packet_id = connection.next_packet_id % 65535 + 1
        packet_id = connection.next_packet_id
        connection.next_packet_id = packet_id % 65535 + 1
        return packet_id

    def _send_publish(self, connection, message, *, attempt_meta):
        packet_id = self._take_packet_id(connection)
        connection.outbox.append(build_publish(packet_id, message.topic,
                                               message.text))
        if attempt_meta is not None:
            group, attempt_id = attempt_meta
            connection.inflight[packet_id] = (message.message, group, attempt_id)
        return packet_id

    # ------------------------------------------------------------- 确认入口
    def acknowledge(self, connection, packet_id, *, at=None):
        """PUBACK 按连接、订阅实例、组和办理引用解释。"""
        if at is not None:
            self.now = at
        mapping = connection.inflight.get(packet_id)
        if mapping is None:
            self._emit("ack-stale", connection=connection.connection_id,
                       packet_id=packet_id)
            raise LateAcknowledgment(
                f"连接 {connection.connection_id} 没有编号 {packet_id} 的在途报文")
        message_id, group, attempt_id = mapping
        delivery = self.deliveries[(message_id, group)]
        attempt = next(item for item in delivery.attempts
                       if item.attempt == attempt_id)
        if delivery.current is not attempt:
            # 新办理引用已经分配：旧连接的迟到确认不能抹掉新队列。
            self._emit("ack-stale", connection=connection.connection_id,
                       packet_id=packet_id, superseded=attempt_id,
                       current=delivery.current.attempt)
            raise LateAcknowledgment(
                f"办理引用 {attempt_id} 已被 {delivery.current.attempt} 取代")
        attempt.observations.append((Observation.ACK, self.now))
        connection.session.pending.discard((message_id, group))
        self._emit("ack", connection=connection.connection_id, message=message_id,
                   group=group, attempt=attempt_id, packet_id=packet_id)
        return AckResult(message_id, group, attempt_id, True)

    # ------------------------------------------------------------- 接班入口
    def handoff(self, connection):
        """值班入口显式接班：复用连接建立时的同一套资格核对。"""
        identity = connection.identity
        rule = self._rule_for(identity)
        old_sessions = [session for session in self._all_sessions_named(
            identity.client_name) if session.pending]
        claimed, refused, quarantined = [], [], []
        for old in old_sessions:
            for key in sorted(old.pending):
                message_id, group = key
                message = self.messages[message_id]
                reason = self._handoff_refusal(rule, identity, old, message, group)
                if reason is None:
                    delivery = self.deliveries[key]
                    previous = delivery.current
                    self._open_new_attempt(delivery, rule, identity,
                                           connection)
                    self._revoke_old_inflight(previous, message_id, group)
                    connection.session.pending.add(key)
                    old.pending.discard(key)
                    claimed.append(message_id)
                elif reason is StopReason.NO_HANDOFF and \
                        message.handoff is HandoffPolicy.INSTANCE_PRIVATE:
                    refused.append({"message": message_id,
                                    "reason": Redelivery.VERIFY_ORIGINAL.value})
                else:
                    delivery = self.deliveries[key]
                    delivery.quarantined = True
                    delivery.stop_reason = reason
                    old.pending.discard(key)
                    quarantined.append({"message": message_id,
                                        "reason": reason.value})
        self._emit("explicit-handoff", connection=connection.connection_id,
                   claimed=claimed, refused=refused, quarantined=quarantined)
        return {"claimed": claimed, "refused": refused,
                "quarantined": quarantined}

    # ------------------------------------------------------------- 保留状态
    def _retained_for(self, identity, topic):
        line, _ = _split_topic(topic)
        record = self.retained.get((topic, identity.tenant, line))
        if record is None:
            return None
        # 设备改由另一租户管理后，旧状态不成为新租户的当前状态。
        if self.ownership_change and self.ownership_change["production_line"] == line:
            change = self.ownership_change
            if identity.tenant == change["to_tenant"] and record.at < change["at"]:
                return None
        return record

    def read_latest(self, identity, topic):
        rule = self._rule_for(identity)
        if rule is None or "latest" not in rule.topics:
            raise PermissionDenied("该身份没有公共状态订阅范围")
        record = self._retained_for(identity, topic)
        if record is not None and not self._eligible(rule, record):
            raise PermissionDenied("保留状态超出当前值班范围或租户不匹配")
        return record

    # ------------------------------------------------------------- 诊断入口
    def _quarantine_message(self, message_id, reason):
        for (mid, group), delivery in self.deliveries.items():
            if mid == message_id and delivery.state == "pending":
                delivery.quarantined = True
                delivery.stop_reason = reason
                self._emit("quarantine", message=mid, group=group,
                           reason=reason.value)

    def redelivery_class(self, message_id, group):
        delivery = self.deliveries[(message_id, group)]
        classified = self._classify(delivery)
        return classified.value if classified else delivery.state

    def unconfirmed(self):
        """列出全部未确认内容及其处置分类。"""
        rows = []
        for (message_id, group), delivery in sorted(self.deliveries.items()):
            if delivery.state in ("pending", "quarantined"):
                classified = self._classify(delivery)
                current = delivery.current
                rows.append({
                    "message": message_id, "group": group,
                    "state": delivery.state,
                    "redelivery": classified.value if classified else delivery.state,
                    "stop_reason": delivery.stop_reason.value
                    if delivery.stop_reason else None,
                    "instance": current.instance if current else None,
                    "attempt": current.attempt if current else None,
                })
        return rows

    def _has_jurisdiction(self, viewer, message):
        if viewer.instance == message.source:
            return True
        rule = self._rule_for(viewer)
        if rule is None:
            return False
        return viewer.tenant == message.tenant and \
            viewer.production_line == message.production_line

    def message_status(self, viewer_device_id, message_id):
        """设备运维查询：此刻还在等哪个组、为何停止交付。"""
        viewer = self.devices[viewer_device_id]
        message = self.messages.get(message_id)
        if message is None:
            raise GatewayError(f"消息 {message_id} 不存在")
        if not self._has_jurisdiction(viewer, message):
            raise PermissionDenied("仅相关管辖方可查看此消息状态")
        viewer_rule = self._rule_for(viewer)
        rows = []
        for (mid, group), delivery in sorted(self.deliveries.items()):
            if mid != message_id:
                continue
            if viewer_rule is not None and viewer_rule.group != group:
                continue  # 组之间互不可见彼此的领取/确认状态
            classified = self._classify(delivery)
            current = delivery.current
            rows.append({
                "group": group,
                "state": delivery.state,
                "waiting_for": group if delivery.state in ("pending", "waiting") else None,
                "redelivery": classified.value if classified else delivery.state,
                "stop_reason": delivery.stop_reason.value if delivery.stop_reason else None,
                "attempt": current.attempt if current else None,
                "instance": current.instance if current else None,
                "packet_id": current.packet_id if current else None,
            })
        return {
            "message": message.message,
            "source": message.source,
            "generation": message.generation,
            "tenant": message.tenant,
            "production_line": message.production_line,
            "topic": message.topic,
            "produced_at": message.at,
            "deliveries": rows,
        }

    # ------------------------------------------------------------- MQTT 字节
    def connect_from_packet(self, device_id, packet_bytes, *, connection_id=None):
        packet = parse_packet(packet_bytes)
        if not isinstance(packet, ConnectPacket):
            raise ProtocolError("首帧必须是 CONNECT")
        result = self.open_connection(
            device_id, clean_session=packet.clean_session,
            client_id=packet.client_id, connection_id=connection_id)
        connection = self.connections.get(result.connection_id)
        # 恢复/接班产生的 DUP 帧已在 open_connection 内入队，这里只回 CONNACK。
        return connection, connack(session_present=result.session_present,
                                   return_code=result.return_code), result

    def handle_frame(self, connection, packet_bytes):
        """处理一帧 MQTT 报文，返回 FrameReply（回送字节与领域结果）。"""
        packet = parse_packet(packet_bytes)
        if isinstance(packet, ConnectPacket):
            raise ProtocolError("CONNECT 请使用 connect_from_packet 建立连接")
        if isinstance(packet, tuple):  # PINGREQ / DISCONNECT 控制标记
            if packet == ("PINGREQ",):
                return FrameReply(pingresp())
            if packet == ("DISCONNECT",):
                self.close_connection(connection.connection_id)
                return FrameReply(disconnect())
        name = type(packet).__name__
        if name == "SubscribePacket":
            granted = []
            for topic, qos in packet.filters:
                ok, _ = self.subscribe(connection, topic, qos=qos)
                granted.append(min(qos, 1) if ok else 0x80)
            return FrameReply(suback(packet.packet_id, granted))
        if name == "PublishPacket":
            if packet.qos != 1:
                raise ProtocolError("本地入口仅协商 QoS 1 发布")
            result = self.publish_device(connection, packet.topic, packet.payload)
            return FrameReply(puback(packet.packet_id), result)
        if name == "PubackPacket":
            result = self.acknowledge(connection, packet.packet_id)
            return FrameReply(b"", result)
        raise ProtocolError(f"未处理的报文 {name}")


def main():
    """端到端演练：用 mqtt_samples 风格字节走通连接、订阅与确认。"""
    parser = argparse.ArgumentParser(description="本地消息入口端到端演练")
    parser.add_argument("--inputs", type=Path,
                        default=Path(__file__).with_name("inputs"))
    args = parser.parse_args()
    from mqtt_samples import connect, publish, subscribe, puback

    gateway = Gateway.from_source(args.inputs, now=45)
    watch, ack_bytes, result = gateway.connect_from_packet(
        "watch-next", connect("watch-1"), connection_id="c-demo")
    print("CONNACK:", ack_bytes.hex(), "session_present =", result.session_present,
          "claimed =", result.claimed)
    reply = watch.feed(subscribe(21, "L-30/alerts"))
    print("SUBACK:", reply.response.hex())
    reply = watch.feed(publish(31, "L-30/alerts", "演练新告警"))
    print("设备侧 PUBACK:", reply.response.hex(),
          "交付:", reply.result.deliveries)
    outgoing = watch.outbox.pop(0)
    print("入口下发 PUBLISH:", outgoing.hex())
    parsed = parse_packet(outgoing)
    reply = watch.feed(puback(parsed.packet_id))
    print("接收侧确认:", reply.result)


if __name__ == "__main__":
    main()
