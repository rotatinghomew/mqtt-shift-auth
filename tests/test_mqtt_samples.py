"""对照 MQTT 联调端的协议字节。"""
import unittest

from mqtt_samples import connect, publish, puback, subscribe


class PacketTest(unittest.TestCase):
    def test_persistent_connect(self):
        self.assertEqual(connect("watch-1"),
                         bytes.fromhex("101300044d5154540400001e000777617463682d31"))

    def test_qos_one_exchange(self):
        self.assertEqual(subscribe(11, "L-30/alerts"),
                         bytes.fromhex("8210000b000b4c2d33302f616c6572747301"))
        self.assertEqual(publish(12, "L-30/alerts", "ok"),
                         bytes.fromhex("3211000b4c2d33302f616c65727473000c6f6b"))
        self.assertEqual(puback(12), bytes.fromhex("4002000c"))

    def test_payload_uses_variable_remaining_length(self):
        packet = publish(19, "alerts", "A" * 160)
        self.assertEqual(packet[:3], bytes.fromhex("32aa01"))
        self.assertEqual(len(packet), 173)
