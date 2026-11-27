"""本地消息入口的身份核验、接续、接班与诊断规则。"""
import json
import unittest
from pathlib import Path

from message_gateway import (Gateway, GatewayError, IdentityRejected,
                             LateAcknowledgment, PermissionDenied, Redelivery,
                             StopReason)
from mqtt_codec import parse_packet
from mqtt_samples import connect, puback, publish, subscribe

ROOT = Path(__file__).resolve().parents[1]
INPUTS = ROOT / "inputs"


def handoff_fixture(*, second_range=(5, 40), second_handoff=True,
                    group_setting="group-only"):
    """两个同 client_name 的接收实例 + 一条组告警 + 一条实例私有告警。"""
    return {
        "devices": {
            "d-old": {"client_name": "watch-1", "tenant": "red",
                      "production_line": "L-30", "instance": "ops-1",
                      "generation": 1, "certificate_verified": True},
            "d-new": {"client_name": "watch-1", "tenant": "red",
                      "production_line": "L-30", "instance": "ops-2",
                      "generation": 2, "certificate_verified": True},
            "d-bad": {"client_name": "watch-1", "tenant": "blue",
                      "production_line": "L-30", "instance": "ops-9",
                      "generation": 1, "certificate_verified": True},
            "d-gen": {"client_name": "watch-1", "tenant": "red",
                      "production_line": "L-30", "instance": "ops-3",
                      "generation": 3, "certificate_verified": True},
            "sensor": {"client_name": "lens-5", "tenant": "red",
                       "production_line": "L-30", "instance": "sensor-1",
                       "generation": 1, "certificate_verified": True},
        },
        "subscriptions": {
            "first": {"group": "maint", "instance": "ops-1", "tenant": "red",
                      "production_line": "L-30", "range": [0, 20],
                      "topics": ["alerts", "latest"], "handoff": True},
            "second": {"group": "maint", "instance": "ops-2", "tenant": "red",
                       "production_line": "L-30", "range": list(second_range),
                       "topics": ["alerts", "latest"],
                       "handoff": second_handoff},
            "blue": {"group": "blue-maint", "instance": "ops-9",
                     "tenant": "blue", "production_line": "L-30",
                     "range": [0, 60], "topics": ["alerts", "latest"],
                     "handoff": False},
            "maint-settings": {"group": "maint",
                               "pending_handoff": group_setting,
                               "unconfirmed_instance_private":
                                   "verify-original", "qos": 1},
        },
        "messages": {
            "group-alert": {"message": "g-1", "source": "sensor-1",
                            "generation": 1, "tenant": "red",
                            "topic": "L-30/alerts", "groups": ["maint"],
                            "handoff": "group", "at": 10, "text": "组告警"},
            "private-alert": {"message": "g-2", "source": "sensor-1",
                              "generation": 1, "tenant": "red",
                              "topic": "L-30/alerts", "groups": ["maint"],
                              "receiver": "ops-1",
                              "handoff": "instance-private", "at": 12,
                              "text": "私有告警"},
        },
        "deliveries": {
            "group-send": {"message": "g-1", "attempt": "try-1",
                           "group": "maint", "instance": "ops-1",
                           "connection": "c-4", "packet_id": 12,
                           "observation": "sent_unknown", "at": 14},
            "private-send": {"message": "g-2", "attempt": "try-2",
                             "group": "maint", "instance": "ops-1",
                             "connection": "c-4", "packet_id": 13,
                             "observation": "sent_unknown", "at": 15},
        },
    }


class IdentityTest(unittest.TestCase):
    def test_unverified_certificate_is_rejected(self):
        data = handoff_fixture()
        data["devices"]["d-old"]["certificate_verified"] = False
        gateway = Gateway.from_records(data)
        with self.assertRaises(IdentityRejected):
            gateway.open_connection("d-old")

    def test_self_reported_client_name_mismatch_rejected(self):
        gateway = Gateway.from_records(handoff_fixture())
        result = gateway.open_connection("d-old", client_id="watch-9")
        self.assertEqual(result.return_code, 2)
        self.assertEqual(result.connection_id, "")

    def test_unknown_device_rejected(self):
        gateway = Gateway.from_records(handoff_fixture())
        with self.assertRaises(IdentityRejected):
            gateway.open_connection("ghost")


class SessionResumeTest(unittest.TestCase):
    def test_same_instance_resume_keeps_packet_id_and_marks_dup(self):
        gateway = Gateway.from_records(handoff_fixture(), now=16)
        result = gateway.open_connection("d-old")
        self.assertTrue(result.session_present)
        resends = [event for event in gateway.events
                   if event["event"] == "resend" and event["message"] == "g-1"]
        self.assertEqual(len(resends), 1)
        self.assertEqual(resends[0]["packet_id"], 12)
        self.assertTrue(resends[0]["duplicate"])
        self.assertEqual(resends[0]["attempt"], "try-1")
        connection = gateway.connections[result.connection_id]
        # 续传沿用原报文号，PUBACK 完成原办理引用。
        ack = gateway.acknowledge(connection, 12)
        self.assertEqual(ack.attempt, "try-1")
        self.assertEqual(gateway.deliveries[("g-1", "maint")].state,
                         "acknowledged")

    def test_clean_session_does_not_resume(self):
        gateway = Gateway.from_records(handoff_fixture())
        result = gateway.open_connection("d-old", clean_session=True)
        self.assertFalse(result.session_present)
        self.assertTrue(gateway.connections[result.connection_id]
                        .session.ephemeral)
        self.assertFalse(any(event["event"] == "resend"
                             for event in gateway.events))


class HandoffTest(unittest.TestCase):
    def test_qualified_handoff_opens_new_attempt_with_new_reference(self):
        gateway = Gateway.from_records(handoff_fixture(), now=18)
        result = gateway.open_connection("d-new")
        self.assertEqual(result.claimed, ("g-1",))
        delivery = gateway.deliveries[("g-1", "maint")]
        self.assertEqual(len(delivery.attempts), 2)
        self.assertEqual(delivery.attempts[0].attempt, "try-1")
        self.assertEqual(delivery.attempts[1].attempt, "try-3")
        self.assertEqual(delivery.attempts[1].instance, "ops-2")
        self.assertEqual(delivery.state, "pending")

    def test_private_message_not_claimable_by_new_member(self):
        gateway = Gateway.from_records(handoff_fixture(), now=18)
        result = gateway.open_connection("d-new")
        self.assertEqual(len(result.refused), 1)
        self.assertEqual(result.refused[0]["key"], ["g-2", "maint"])
        self.assertEqual(result.refused[0]["reason"],
                         Redelivery.VERIFY_ORIGINAL.value)
        delivery = gateway.deliveries[("g-2", "maint")]
        self.assertEqual(delivery.state, "pending")
        self.assertFalse(delivery.quarantined)
        self.assertEqual(len(delivery.attempts), 1)
        self.assertEqual(gateway.redelivery_class("g-2", "maint"),
                         Redelivery.VERIFY_ORIGINAL.value)

    def test_tenant_change_cannot_claim_old_queue(self):
        gateway = Gateway.from_records(handoff_fixture(), now=18)
        result = gateway.open_connection("d-bad")
        self.assertEqual(result.claimed, ())
        group_delivery = gateway.deliveries[("g-1", "maint")]
        self.assertTrue(group_delivery.quarantined)
        self.assertEqual(group_delivery.stop_reason,
                         StopReason.TENANT_MISMATCH)
        # 私有告警不交接也不隔离，只能向原订阅者核对。
        private = gateway.deliveries[("g-2", "maint")]
        self.assertFalse(private.quarantined)

    def test_out_of_handoff_range_is_quarantined(self):
        # 接班人班次与原班 [0,20] 不衔接，组覆盖无法接续该消息。
        gateway = Gateway.from_records(
            handoff_fixture(second_range=(30, 40)), now=18)
        result = gateway.open_connection("d-new")
        self.assertEqual(result.claimed, ())
        delivery = gateway.deliveries[("g-1", "maint")]
        self.assertTrue(delivery.quarantined)
        self.assertEqual(delivery.stop_reason, StopReason.OUT_OF_RANGE)

    def test_group_only_setting_governs_handoff(self):
        gateway = Gateway.from_records(
            handoff_fixture(group_setting="disabled"), now=18)
        result = gateway.open_connection("d-new")
        self.assertEqual(result.claimed, ())
        delivery = gateway.deliveries[("g-1", "maint")]
        self.assertTrue(delivery.quarantined)
        self.assertEqual(delivery.stop_reason, StopReason.NO_HANDOFF)

    def test_handoff_disabled_on_successor_quarantines(self):
        gateway = Gateway.from_records(
            handoff_fixture(second_handoff=False), now=18)
        result = gateway.open_connection("d-new")
        self.assertEqual(result.claimed, ())
        delivery = gateway.deliveries[("g-1", "maint")]
        self.assertEqual(delivery.stop_reason, StopReason.NO_HANDOFF)

    def test_new_generation_without_duty_registration_quarantines(self):
        gateway = Gateway.from_records(handoff_fixture(), now=18)
        result = gateway.open_connection("d-gen")
        self.assertEqual(result.claimed, ())
        for key in (("g-1", "maint"), ("g-2", "maint")):
            delivery = gateway.deliveries[key]
            self.assertTrue(delivery.quarantined)
            self.assertEqual(delivery.stop_reason,
                             StopReason.DEVICE_GENERATION)

    def test_device_outbox_quarantined_on_generation_change(self):
        gateway = Gateway.from_records(handoff_fixture(), now=18)
        old_session = gateway.sessions["watch-1@ops-1"]
        old_session.outbox.append({"message": "g-1", "packet_id": 7,
                                   "topic": "L-30/alerts", "payload": "x"})
        gateway.open_connection("d-gen")
        self.assertEqual(old_session.outbox, [])
        self.assertTrue(old_session.closed)
        delivery = gateway.deliveries[("g-1", "maint")]
        self.assertEqual(delivery.stop_reason,
                         StopReason.DEVICE_GENERATION)

    def test_handoff_survives_previous_process_exit(self):
        gateway = Gateway.from_records(handoff_fixture(), now=16)
        first = gateway.open_connection("d-old")
        gateway.close_connection(first.connection_id)  # 值班进程退出
        second = gateway.open_connection("d-new")
        gateway.now = 18
        self.assertEqual(second.claimed, ("g-1",))

    def test_explicit_handoff_entry_point(self):
        gateway = Gateway.from_records(handoff_fixture(), now=18)
        connection_id = gateway.open_connection("d-new").connection_id
        connection = gateway.connections[connection_id]
        report = gateway.handoff(connection)
        self.assertEqual(report["claimed"], ["g-1"])
        self.assertEqual(report["refused"][0]["reason"],
                         Redelivery.VERIFY_ORIGINAL.value)


class SubscriptionTest(unittest.TestCase):
    def setUp(self):
        self.gateway = Gateway.from_source(INPUTS, now=20)

    def test_alerts_and_latest_are_independent_subscriptions(self):
        result = self.gateway.open_connection("watch-old")
        connection = self.gateway.connections[result.connection_id]
        ok, group = self.gateway.subscribe(connection, "L-30/alerts")
        self.assertTrue(ok)
        self.assertEqual(group, "maint")
        ok, _ = self.gateway.subscribe(connection, "L-30/latest")
        self.assertTrue(ok)
        ok, reason = self.gateway.subscribe(connection, "L-30/secret")
        self.assertFalse(ok)

    def test_identity_without_duty_registration_denied(self):
        result = self.gateway.open_connection("sensor-old")
        connection = self.gateway.connections[result.connection_id]
        ok, reason = self.gateway.subscribe(connection, "L-30/alerts")
        self.assertFalse(ok)
        self.assertEqual(reason, StopReason.NOT_A_RECEIVER.value)

    def test_retained_latest_follows_tenant_and_ownership_change(self):
        red = self.gateway.connections[
            self.gateway.open_connection("watch-old").connection_id]
        red_latest = self.gateway.read_latest(red.identity, "L-30/latest")
        self.assertEqual(red_latest.message, "s-15")
        blue = self.gateway.connections[
            self.gateway.open_connection("watch-neighbor").connection_id]
        blue_latest = self.gateway.read_latest(blue.identity, "L-30/latest")
        # 蓝租户只能看到 35 秒交接后的新状态，旧红状态不成为当前状态。
        self.assertEqual(blue_latest.message, "s-22")
        # 红租户仍按自己的归属看到旧状态。
        red_identity = self.gateway.devices["watch-old"]
        self.assertEqual(
            self.gateway._retained_for(red_identity, "L-30/latest").message,
            "s-15")

    def test_withdraw_private_pending_quarantines_but_keeps_records(self):
        self.gateway.withdraw_subscription("ops-11", "alerts", at=22)
        delivery = self.gateway.deliveries[("a-802", "maint")]
        self.assertTrue(delivery.quarantined)
        self.assertEqual(delivery.stop_reason,
                         StopReason.SUBSCRIPTION_WITHDRAWN)
        # 已交付的原始收件记录保留。
        self.assertEqual(delivery.attempts[0].attempt, "try-4")
        self.assertEqual(delivery.attempts[0].connection_id, "c-4")

    def test_withdraw_group_pending_keeps_group_claim_possible(self):
        gateway = Gateway.from_records(handoff_fixture(), now=16)
        gateway.withdraw_subscription("ops-1", "alerts")
        delivery = gateway.deliveries[("g-1", "maint")]
        self.assertEqual(delivery.state, "pending")
        self.assertFalse(delivery.quarantined)
        result = gateway.open_connection("d-new")
        self.assertEqual(result.claimed, ("g-1",))


class DeliveryAckTest(unittest.TestCase):
    def test_group_acks_are_independent(self):
        import json
        full = {path.stem: json.loads(path.read_text())
                for path in INPUTS.glob("*.json")}
        # 只有 quality 已确认，maint 仍 sent_unknown：两组互不影响。
        full["deliveries"] = {
            key: row for key, row in full["deliveries"].items()
            if key in ("maint-send", "qual-ack")}
        gateway = Gateway.from_records(full, now=30)
        self.assertEqual(gateway.deliveries[("a-801", "quality")].state,
                         "acknowledged")
        self.assertEqual(gateway.deliveries[("a-801", "maint")].state,
                         "pending")

    def test_late_ack_cannot_erase_new_queue(self):
        gateway = Gateway.from_records(handoff_fixture(), now=14)
        old = gateway.connections[
            gateway.open_connection("d-old").connection_id]
        # 先在旧连接上制造一条在途的组告警。
        sensor = gateway.connections[
            gateway.open_connection("sensor").connection_id]
        gateway.subscribe(sensor, "L-30/alerts")
        published = gateway.publish_device(
            sensor, "L-30/alerts", "新告警", message_id="g-9", at=15)
        self.assertEqual(published.deliveries["maint"], "pending")
        packet_id = next(iter(old.inflight))
        # 新实例接班，分配新的办理引用。
        gateway.open_connection("d-new")
        delivery = gateway.deliveries[("g-9", "maint")]
        new_attempt = delivery.current
        self.assertNotEqual(new_attempt.connection_id, old.connection_id)
        with self.assertRaises(LateAcknowledgment):
            gateway.acknowledge(old, packet_id)
        self.assertEqual(delivery.current, new_attempt)
        self.assertEqual(delivery.state, "pending")

    def test_packet_identifier_may_repeat_in_new_session(self):
        gateway = Gateway.from_records(handoff_fixture(), now=14)
        old = gateway.connections[
            gateway.open_connection("d-old").connection_id]
        self.assertEqual(old.inflight.get(12)[:2], ("g-1", "maint"))
        gateway.open_connection("d-new")
        new_attempt = gateway.deliveries[("g-1", "maint")].current
        # 新会话独立编号空间，允许再次分到 1；报文号相同但办理引用不同。
        self.assertEqual(new_attempt.packet_id, 1)
        self.assertNotEqual(new_attempt.attempt, "try-1")

    def test_redelivery_classification(self):
        gateway = Gateway.from_records(handoff_fixture(), now=16)
        self.assertEqual(gateway.redelivery_class("g-1", "maint"),
                         Redelivery.SAFE_REDELIVER.value)
        self.assertEqual(gateway.redelivery_class("g-2", "maint"),
                         Redelivery.VERIFY_ORIGINAL.value)
        gateway.withdraw_subscription("ops-1", "alerts")
        private = gateway.deliveries[("g-2", "maint")]
        private.quarantined = True
        private.stop_reason = StopReason.SUBSCRIPTION_WITHDRAWN
        self.assertEqual(gateway.redelivery_class("g-2", "maint"),
                         Redelivery.QUARANTINE.value)
        rows = {row["message"]: row for row in gateway.unconfirmed()}
        self.assertEqual(rows["g-1"]["redelivery"],
                         Redelivery.SAFE_REDELIVER.value)


class LiveFlowTest(unittest.TestCase):
    def test_qos_one_round_trip_over_mqtt_bytes(self):
        gateway = Gateway.from_records(handoff_fixture(), now=14)
        connection, connack_bytes, result = gateway.connect_from_packet(
            "d-old", connect("watch-1"))
        self.assertEqual(connack_bytes, bytes.fromhex("20020100"))
        reply = connection.feed(subscribe(21, "L-30/alerts"))
        self.assertEqual(reply.response[:1], bytes.fromhex("90"))
        self.assertEqual(reply.response[-1], 0x01)
        sensor, _, _ = gateway.connect_from_packet(
            "sensor", connect("lens-5"))
        reply = sensor.feed(publish(31, "L-30/alerts", "新告警"))
        self.assertEqual(reply.response, puback(31))
        # 连接恢复时先带着 g-1/g-2 的 DUP 续传帧，新告警排在其后。
        for _ in range(2):
            self.assertTrue(parse_packet(connection.outbox.pop(0)).duplicate)
        outgoing = connection.outbox.pop(0)
        parsed = parse_packet(outgoing)
        self.assertEqual(parsed.topic, "L-30/alerts")
        self.assertEqual(parsed.payload, "新告警".encode("utf-8"))
        reply = connection.feed(puback(parsed.packet_id))
        self.assertTrue(reply.result.accepted)

    def test_subscribe_denial_encoded_in_suback(self):
        gateway = Gateway.from_records(handoff_fixture())
        sensor, _, _ = gateway.connect_from_packet(
            "sensor", connect("lens-5"))
        reply = sensor.feed(subscribe(5, "L-30/alerts"))
        self.assertEqual(reply.response.hex().endswith("80"), True)

    def test_ping_and_disconnect_frames(self):
        gateway = Gateway.from_records(handoff_fixture())
        connection, _, _ = gateway.connect_from_packet(
            "d-old", connect("watch-1"))
        self.assertEqual(connection.feed(bytes.fromhex("c000")).response,
                         bytes.fromhex("d000"))
        reply = connection.feed(bytes.fromhex("e000"))
        self.assertEqual(reply.response, bytes.fromhex("e000"))
        self.assertFalse(connection.open)

    def test_waiting_delivery_flushes_when_subscription_opens(self):
        gateway = Gateway.from_records(handoff_fixture(), now=14)
        sensor = gateway.connections[
            gateway.open_connection("sensor").connection_id]
        # 此刻还没有 maint 接收端在线。
        result = gateway.publish_device(
            sensor, "L-30/alerts", "等待订阅", message_id="g-8")
        self.assertEqual(result.deliveries["maint"], "waiting")
        watch, _, _ = gateway.connect_from_packet(
            "d-old", connect("watch-1"))
        watch.feed(subscribe(9, "L-30/alerts"))
        delivery = gateway.deliveries[("g-8", "maint")]
        self.assertEqual(delivery.state, "pending")
        self.assertEqual(delivery.current.instance, "ops-1")

    def test_retained_latest_is_pushed_on_subscribe(self):
        from mqtt_codec import parse_packet
        gateway = Gateway.from_source(INPUTS, now=20)
        watch, _, _ = gateway.connect_from_packet(
            "watch-old", connect("watch-1"))
        before = len(watch.outbox)
        watch.feed(subscribe(3, "L-30/latest"))
        pushed = [parse_packet(frame) for frame in watch.outbox[before:]]
        self.assertTrue(any(packet.topic == "L-30/latest"
                            for packet in pushed))

    def test_instance_private_live_publish_is_verify_original(self):
        gateway = Gateway.from_records(handoff_fixture(), now=14)
        sensor = gateway.connections[
            gateway.open_connection("sensor").connection_id]
        watch = gateway.connections[
            gateway.open_connection("d-old").connection_id]
        gateway.subscribe(watch, "L-30/alerts")
        result = gateway.publish_device(
            sensor, "L-30/alerts", "私密", message_id="g-7",
            receiver="ops-1", handoff="instance-private")
        self.assertEqual(result.deliveries["maint"], "pending")
        self.assertEqual(gateway.messages["g-7"].receiver, "ops-1")
        self.assertEqual(gateway.redelivery_class("g-7", "maint"),
                         Redelivery.VERIFY_ORIGINAL.value)

    def test_message_outside_subscriber_range_waits_instead_of_misdelivering(self):
        gateway = Gateway.from_records(handoff_fixture(), now=14)
        sensor = gateway.connections[
            gateway.open_connection("sensor").connection_id]
        watch = gateway.connections[
            gateway.open_connection("d-old").connection_id]
        gateway.subscribe(watch, "L-30/alerts")
        result = gateway.publish_device(
            sensor, "L-30/alerts", "越界", message_id="g-6", at=25)
        self.assertEqual(result.deliveries["maint"], "waiting")
        self.assertEqual(
            gateway.deliveries[("g-6", "maint")].state, "waiting")


class DiagnosticsTest(unittest.TestCase):
    def setUp(self):
        self.gateway = Gateway.from_source(INPUTS, now=45)

    def test_status_shows_waiting_group_and_stop_reason(self):
        status = self.gateway.message_status("sensor-old", "a-802")
        self.assertEqual(status["source"], "sensor-R17")
        self.assertEqual(len(status["deliveries"]), 1)
        row = status["deliveries"][0]
        self.assertEqual(row["group"], "maint")
        self.assertEqual(row["state"], "pending")
        self.assertEqual(row["waiting_for"], "maint")
        self.assertEqual(row["redelivery"],
                         Redelivery.VERIFY_ORIGINAL.value)

    def test_group_cannot_see_other_group_status(self):
        status = self.gateway.message_status("watch-old", "a-801")
        groups = {row["group"] for row in status["deliveries"]}
        self.assertEqual(groups, {"maint"})
        quality_status = self.gateway.message_status(
            "quality-receiver", "a-801")
        self.assertEqual(
            {row["group"] for row in quality_status["deliveries"]},
            {"quality"})

    def test_foreign_tenant_denied_diagnostics(self):
        with self.assertRaises(PermissionDenied):
            self.gateway.message_status("sensor-new", "a-801")
        with self.assertRaises(PermissionDenied):
            self.gateway.message_status("watch-neighbor", "a-802")

    def test_quarantine_reason_visible_to_jurisdiction(self):
        gateway = Gateway.from_records(handoff_fixture(), now=18)
        gateway.open_connection("d-bad")
        status = gateway.message_status("sensor", "g-1")
        row = status["deliveries"][0]
        self.assertEqual(row["state"], "quarantined")
        self.assertEqual(row["stop_reason"],
                         StopReason.TENANT_MISMATCH.value)

    def test_unknown_message_is_error_not_permission(self):
        with self.assertRaises(GatewayError):
            self.gateway.message_status("sensor-old", "nope")


if __name__ == "__main__":
    unittest.main()
