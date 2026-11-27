"""本地演练使用的 MQTT 3.1.1 报文解析与构造。

只实现联调所需的报文：CONNECT/CONNACK、SUBSCRIBE/SUBACK、
QoS 1 的 PUBLISH、PUBACK，以及保活与断开。构造出的字节与
:mod:`mqtt_samples` 生成的样本完全互通，因此旧客户端的报文可以
直接被本地消息入口解析。
"""
from dataclasses import dataclass
import struct

from mqtt_samples import frame, puback, string


class ProtocolError(ValueError):
    """报文不符合本演练约定的 MQTT 3.1.1 形态。"""


@dataclass(frozen=True)
class ConnectPacket:
    client_id: str
    clean_session: bool
    keep_alive: int
    protocol_level: int


@dataclass(frozen=True)
class SubscribePacket:
    packet_id: int
    filters: tuple[tuple[str, int], ...]


@dataclass(frozen=True)
class PublishPacket:
    topic: str
    payload: bytes
    qos: int
    packet_id: int | None = None
    duplicate: bool = False
    retain: bool = False


@dataclass(frozen=True)
class PubackPacket:
    packet_id: int


class _Reader:
    def __init__(self, data):
        self.data = data
        self.pos = 0

    def take(self, count):
        if self.pos + count > len(self.data):
            raise ProtocolError("报文长度不足")
        chunk = self.data[self.pos:self.pos + count]
        self.pos += count
        return chunk

    def u16(self):
        return struct.unpack(">H", self.take(2))[0]

    def text(self):
        length = self.u16()
        return self.take(length).decode("utf-8")

    def rest(self):
        chunk = self.data[self.pos:]
        self.pos = len(self.data)
        return chunk


def _decode_length(data, start):
    multiplier = 1
    value = 0
    index = start
    for _ in range(4):
        if index >= len(data):
            raise ProtocolError("缺少剩余长度字段")
        byte = data[index]
        index += 1
        value += (byte & 0x7F) * multiplier
        if not byte & 0x80:
            return value, index
        multiplier *= 128
    raise ProtocolError("剩余长度字段过长")


def parse_packet(data: bytes):
    """解析一帧 MQTT 3.1.1 报文，返回对应的报文数据类。"""
    if not data:
        raise ProtocolError("空报文")
    first = data[0]
    packet_type = first >> 4
    flags = first & 0x0F
    remaining, body_start = _decode_length(data, 1)
    if body_start + remaining != len(data):
        raise ProtocolError("报文实际长度与剩余长度不一致")
    reader = _Reader(data[body_start:body_start + remaining])

    if packet_type == 1:
        if flags:
            raise ProtocolError("CONNECT 固定头标志位必须为 0")
        protocol = reader.text()
        level = reader.take(1)[0]
        if protocol != "MQTT" or level != 4:
            raise ProtocolError("只支持 MQTT 3.1.1")
        connect_flags = reader.take(1)[0]
        if connect_flags & 0xFC:
            raise ProtocolError("本演练不使用遗嘱、用户名或口令标志")
        keep_alive = reader.u16()
        client_id = reader.text()
        if not client_id:
            raise ProtocolError("客户端名不能为空")
        return ConnectPacket(client_id, bool(connect_flags & 0x02), keep_alive, level)

    if packet_type == 3:
        qos = (flags >> 1) & 0x03
        if qos == 2:
            raise ProtocolError("本演练不协商 QoS 2")
        topic = reader.text()
        packet_id = reader.u16() if qos else None
        if qos and packet_id == 0:
            raise ProtocolError("报文编号不能为 0")
        return PublishPacket(topic, reader.rest(), qos, packet_id,
                             duplicate=bool(flags & 0x08), retain=bool(flags & 0x01))

    if packet_type == 4:
        if flags or remaining != 2:
            raise ProtocolError("PUBACK 形态非法")
        return PubackPacket(reader.u16())

    if packet_type == 8:
        if flags != 0x02:
            raise ProtocolError("SUBSCRIBE 固定头标志位非法")
        packet_id = reader.u16()
        filters = []
        while reader.pos < len(reader.data):
            topic = reader.text()
            qos = reader.take(1)[0]
            if qos not in (0, 1):
                raise ProtocolError("只支持 QoS 0/1 订阅")
            filters.append((topic, qos))
        if not filters:
            raise ProtocolError("SUBSCRIBE 至少包含一个主题")
        return SubscribePacket(packet_id, tuple(filters))

    if packet_type == 12:
        return ("PINGREQ",)
    if packet_type == 14:
        return ("DISCONNECT",)
    raise ProtocolError(f"本演练不支持报文类型 {packet_type}")


def connack(*, session_present=False, return_code=0):
    """构造 CONNACK。return_code 采用 MQTT 3.1.1 的拒绝码。"""
    return frame(0x20, bytes((0x01 if session_present else 0x00, return_code)))


def suback(packet_id, granted_qos):
    """granted_qos 为 0/1，或 0x80 表示拒绝该订阅。"""
    if isinstance(granted_qos, int):
        granted_qos = [granted_qos]
    return frame(0x90, struct.pack(">H", packet_id) + bytes(granted_qos))


def build_publish(packet_id, topic, payload, *, qos=1, duplicate=False, retain=False):
    if isinstance(payload, str):
        payload = payload.encode("utf-8")
    header = 0x30 | ((0x08 if duplicate else 0) | (qos << 1) | (0x01 if retain else 0))
    body = string(topic)
    if qos:
        body += struct.pack(">H", packet_id)
    return frame(header, body + payload)


def pingresp():
    return frame(0xD0, b"")


def disconnect():
    return frame(0xE0, b"")


__all__ = ["ProtocolError", "ConnectPacket", "SubscribePacket", "PublishPacket",
           "PubackPacket", "parse_packet", "connack", "suback", "build_publish",
           "puback", "pingresp", "disconnect"]
