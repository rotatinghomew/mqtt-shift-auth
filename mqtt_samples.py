"""生成带有本机主题和会话名的 MQTT 3.1.1 联调报文。"""
import argparse
import struct


def string(value):
    data = value.encode("utf-8")
    return struct.pack(">H", len(data)) + data


def frame(header, body):
    remaining = len(body)
    encoded = bytearray([header])
    while remaining >= 128:
        encoded.append((remaining % 128) | 0x80)
        remaining //= 128
    encoded.append(remaining)
    return bytes(encoded) + body


def connect(client_name, *, clean_session=False):
    flags = 0x02 if clean_session else 0
    return frame(0x10, string("MQTT") + bytes((4, flags)) + struct.pack(">H", 30) + string(client_name))


def subscribe(packet_id, topic):
    return frame(0x82, struct.pack(">H", packet_id) + string(topic) + b"\x01")


def publish(packet_id, topic, payload, *, duplicate=False):
    header = 0x3A if duplicate else 0x32
    return frame(header, string(topic) + struct.pack(">H", packet_id) + payload.encode("utf-8"))


def puback(packet_id):
    return frame(0x40, struct.pack(">H", packet_id))


def main():
    parser = argparse.ArgumentParser(description="打印 MQTT 联调报文十六进制")
    parser.add_argument("--client", default="watch-1")
    parser.add_argument("--topic", default="L-30/alerts")
    args = parser.parse_args()
    for packet in (connect(args.client), subscribe(11, args.topic),
                   publish(12, args.topic, "测量重复性待复核"), puback(12)):
        print(packet.hex())


if __name__ == "__main__":
    main()
