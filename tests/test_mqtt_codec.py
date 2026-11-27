"""MQTT 3.1.1 报文解析与 mqtt_samples 字节互通。"""
import unittest

from mqtt_codec import (ConnectPacket, ProtocolError, PubackPacket,
                        PublishPacket, SubscribePacket, build_publish, connack,
                        parse_packet, suback)
from mqtt_samples import connect, puback, publish, subscribe


class CodecTest(unittest.TestCase):
    def test_parse_sample_connect(self):
        packet = parse_packet(connect("watch-1"))
        self.assertIsInstance(packet, ConnectPacket)
        self.assertEqual(packet.client_id, "watch-1")
        self.assertFalse(packet.clean_session)
        self.assertEqual(packet.protocol_level, 4)

    def test_parse_clean_connect(self):
        packet = parse_packet(connect("lens-5", clean_session=True))
        self.assertTrue(packet.clean_session)

    def test_parse_subscribe_qos_one(self):
        packet = parse_packet(subscribe(11, "L-30/alerts"))
        self.assertIsInstance(packet, SubscribePacket)
        self.assertEqual(packet.packet_id, 11)
        self.assertEqual(packet.filters, (("L-30/alerts", 1),))

    def test_parse_qos_one_publish_and_ack(self):
        packet = parse_packet(publish(12, "L-30/alerts", "ok"))
        self.assertIsInstance(packet, PublishPacket)
        self.assertEqual(packet.topic, "L-30/alerts")
        self.assertEqual(packet.payload, b"ok")
        self.assertEqual(packet.qos, 1)
        self.assertEqual(packet.packet_id, 12)
        self.assertFalse(packet.duplicate)
        ack = parse_packet(puback(12))
        self.assertIsInstance(ack, PubackPacket)
        self.assertEqual(ack.packet_id, 12)

    def test_dup_flag_round_trip(self):
        packet = parse_packet(publish(12, "L-30/alerts", "ok", duplicate=True))
        self.assertTrue(packet.duplicate)
        self.assertEqual(build_publish(12, "L-30/alerts", "ok", duplicate=True),
                         publish(12, "L-30/alerts", "ok", duplicate=True))

    def test_multi_filter_subscribe(self):
        from mqtt_samples import frame, string
        import struct
        body = struct.pack(">H", 7) + string("L-30/alerts") + b"\x01" + \
            string("L-30/latest") + b"\x00"
        packet = parse_packet(frame(0x82, body))
        self.assertEqual(packet.filters,
                         (("L-30/alerts", 1), ("L-30/latest", 0)))

    def test_connack_and_suback_bytes(self):
        self.assertEqual(connack(session_present=True),
                         bytes.fromhex("20020100"))
        self.assertEqual(connack(return_code=5), bytes.fromhex("20020005"))
        self.assertEqual(suback(11, [1, 0x80]), bytes.fromhex("9004000b0180"))

    def test_reject_non_311(self):
        with self.assertRaises(ProtocolError):
            parse_packet(bytes.fromhex("100a00034d51540400001e0000"))
        with self.assertRaises(ProtocolError):
            parse_packet(bytes.fromhex("100c00044d5154540500001e0000"))

    def test_reject_qos_two(self):
        # QoS 2 的 PUBLISH 固定头
        bad = bytes.fromhex("340c000b4c2d33302f616c657274000c00")
        with self.assertRaises(ProtocolError):
            parse_packet(bad)

    def test_variable_length_payload(self):
        packet = parse_packet(build_publish(19, "alerts", "A" * 160))
        self.assertEqual(packet.payload, b"A" * 160)

    def test_ping_and_disconnect(self):
        self.assertEqual(parse_packet(bytes.fromhex("c000")), ("PINGREQ",))
        self.assertEqual(parse_packet(bytes.fromhex("e000")), ("DISCONNECT",))


if __name__ == "__main__":
    unittest.main()
