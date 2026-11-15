import json
import tempfile
import unittest
from pathlib import Path

from field_observatory.client import Client
from field_observatory.origin import serve
from field_observatory.reception import drill, flush_acks, load_envelopes, subscribe_once
from field_observatory.subscriptions import (
    SEQ_MAX,
    Receiver,
    parse_envelope,
    seq_forward_distance,
    seq_next,
)


def notification(handle=11, value=1.0, quality="Good", source_time="2026-11-10T10:00:00Z"):
    return {"handle": handle, "value": value, "quality": quality, "source_time": source_time}


def envelope(gateway="station-a", generation="g1", sequence=1, keepalive=False,
             available=None, bindings=None, notifications=None, subscription_id=7):
    raw = {
        "gateway": gateway,
        "subscription_generation": generation,
        "sequence": sequence,
        "keepalive": keepalive,
        "notifications": notifications if notifications is not None else [],
    }
    if available is not None:
        raw["available"] = available
    if bindings is not None:
        raw["bindings"] = bindings
    if subscription_id is not None:
        raw["subscription_id"] = subscription_id
    return raw


class SequenceMathTests(unittest.TestCase):
    def test_next_wraps_to_one_skipping_zero(self):
        self.assertEqual(seq_next(1), 2)
        self.assertEqual(seq_next(SEQ_MAX), 1)

    def test_forward_distance_handles_wrap(self):
        self.assertEqual(seq_forward_distance(5, 5), 0)
        self.assertEqual(seq_forward_distance(1, 3), 2)
        self.assertEqual(seq_forward_distance(SEQ_MAX, 1), 1)
        self.assertEqual(seq_forward_distance(SEQ_MAX - 1, 1), 2)
        self.assertGreater(seq_forward_distance(3, 1), SEQ_MAX // 2)


class EnvelopeValidationTests(unittest.TestCase):
    def test_minimal_envelope_defaults(self):
        parsed = parse_envelope({"gateway": "s", "subscription_generation": "g", "sequence": 1})
        self.assertFalse(parsed.keepalive)
        self.assertIsNone(parsed.available)
        self.assertEqual(parsed.bindings, {})
        self.assertEqual(parsed.notifications, ())

    def test_identity_and_sequence_are_checked(self):
        bad = [
            {"subscription_generation": "g", "sequence": 1},
            {"gateway": " ", "subscription_generation": "g", "sequence": 1},
            {"gateway": "s", "subscription_generation": "g"},
            {"gateway": "s", "subscription_generation": "g", "sequence": 0},
            {"gateway": "s", "subscription_generation": "g", "sequence": SEQ_MAX + 1},
            {"gateway": "s", "subscription_generation": "g", "sequence": True},
            {"gateway": "s", "subscription_generation": "g", "sequence": "3"},
        ]
        for raw in bad:
            with self.subTest(raw=raw), self.assertRaises(ValueError):
                parse_envelope(raw)

    def test_keepalive_must_not_carry_data(self):
        raw = envelope(sequence=2, keepalive=True, notifications=[notification()])
        with self.assertRaisesRegex(ValueError, "keepalive"):
            parse_envelope(raw)

    def test_notification_fields_are_checked(self):
        for item in (
            {"value": 1, "quality": "Good", "source_time": "2026-11-10T10:00:00Z"},
            {"handle": True, "value": 1, "quality": "Good", "source_time": "2026-11-10T10:00:00Z"},
            {"handle": -1, "value": 1, "quality": "Good", "source_time": "2026-11-10T10:00:00Z"},
            {"handle": 11, "quality": "Good", "source_time": "2026-11-10T10:00:00Z"},
            {"handle": 11, "value": 1, "quality": "", "source_time": "2026-11-10T10:00:00Z"},
            {"handle": 11, "value": 1, "quality": "Good", "source_time": "not-a-time"},
        ):
            with self.subTest(item=item), self.assertRaises(ValueError):
                parse_envelope(envelope(notifications=[item]))

    def test_duplicate_handle_in_one_message_is_rejected(self):
        raw = envelope(notifications=[notification(handle=11), notification(handle=11, value=2.0)])
        with self.assertRaisesRegex(ValueError, "duplicate handle"):
            parse_envelope(raw)

    def test_bindings_are_normalized_to_integer_handles(self):
        parsed = parse_envelope(envelope(bindings={"11": "temperature"}))
        self.assertEqual(parsed.bindings, {11: "temperature"})
        for bindings in (["11"], {"x": "temperature"}, {"11": "  "}):
            with self.subTest(bindings=bindings), self.assertRaises(ValueError):
                parse_envelope(envelope(bindings=bindings))

    def test_available_must_be_sequence_list(self):
        with self.assertRaises(ValueError):
            parse_envelope(envelope(available=[1, 0]))


class ReceiverTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.path = Path(self.temporary.name) / "reception.sqlite3"
        self.receiver = Receiver(self.path)

    def tearDown(self):
        self.receiver.close()
        self.temporary.cleanup()

    def view(self, gateway="station-a", generation="g1"):
        station = self.receiver.station_view(gateway)
        for item in station["generations"]:
            if item["generation"] == generation:
                return item
        raise AssertionError(f"generation not found: {generation}")

    def test_observation_keeps_quality_and_source_time(self):
        result = self.receiver.ingest(envelope(
            bindings={"11": "temperature"},
            notifications=[notification(value=12.2, quality="Uncertain", source_time="2026-11-10T10:00:02Z")],
        ))
        self.assertFalse(result.duplicate)
        self.assertEqual(result.observations, 1)
        (observation,) = self.view()["observations"]
        self.assertEqual(observation["sensor"], "temperature")
        self.assertEqual(observation["value"], 12.2)
        self.assertEqual(observation["quality"], "Uncertain")
        self.assertEqual(observation["source_time"], "2026-11-10T10:00:02Z")
        self.assertTrue(observation["resolved"])
        self.assertEqual(len(self.receiver.queued_acks()), 1)

    def test_keepalive_is_not_data_but_is_acknowledged(self):
        self.receiver.ingest(envelope(sequence=5, keepalive=True, bindings={"11": "humidity"}))
        view = self.view()
        self.assertEqual(view["observations"], [])
        self.assertEqual(view["keepalive_sequences"], [5])
        self.assertEqual(view["received_sequences"], [5])
        self.assertEqual(len(self.receiver.queued_acks()), 1)

    def test_missing_sequence_becomes_permanent_gap_without_fabrication(self):
        self.receiver.ingest(envelope(sequence=1, notifications=[notification(value=1.0)]))
        self.receiver.ingest(envelope(sequence=3, notifications=[notification(value=3.0)]))
        view = self.view()
        self.assertEqual([gap["sequence"] for gap in view["recoverable_sequences"]], [2])
        self.assertTrue(self.receiver.mark_gap_permanent("station-a", "g1", 2, "BadMessageNotAvailable"))
        view = self.view()
        self.assertEqual(view["recoverable_sequences"], [])
        self.assertEqual([gap["sequence"] for gap in view["permanent_gaps"]], [2])
        self.assertEqual([obs["sequence"] for obs in view["observations"]], [1, 3])

    def test_late_delivery_fills_gap(self):
        self.receiver.ingest(envelope(sequence=1, notifications=[notification(value=1.0)]))
        self.receiver.ingest(envelope(sequence=3, notifications=[notification(value=3.0)]))
        result = self.receiver.ingest(envelope(sequence=2, notifications=[notification(value=2.0)]))
        self.assertTrue(result.recovered)
        view = self.view()
        self.assertEqual(view["recoverable_sequences"], [])
        self.assertEqual([obs["sequence"] for obs in view["observations"]], [1, 2, 3])

    def test_redelivery_does_not_duplicate_observation(self):
        raw = envelope(bindings={"11": "temperature"}, notifications=[notification(value=12.2)])
        first = self.receiver.ingest(raw)
        second = self.receiver.ingest(raw)
        self.assertFalse(first.duplicate)
        self.assertTrue(second.duplicate)
        self.assertEqual(len(self.view()["observations"]), 1)
        self.assertEqual(len(self.receiver.queued_acks()), 1)
        self.assertEqual(self.receiver.events("conflicting_redelivery"), [])

    def test_conflicting_redelivery_is_logged_and_first_content_kept(self):
        self.receiver.ingest(envelope(notifications=[notification(value=1.0)]))
        changed = envelope(notifications=[notification(value=9.9)])
        result = self.receiver.ingest(changed)
        self.assertTrue(result.duplicate)
        events = self.receiver.events("conflicting_redelivery")
        self.assertEqual(len(events), 1)
        self.assertIn("notifications", events[0]["detail"]["fields"])
        (observation,) = self.view()["observations"]
        self.assertEqual(observation["value"], 1.0)

    def test_handle_reuse_across_generations_stays_separate(self):
        self.receiver.ingest(envelope(generation="g1", sequence=1, bindings={"11": "temperature"},
                                      notifications=[notification(value=12.2)]))
        self.receiver.ingest(envelope(generation="g2", sequence=1, bindings={"11": "humidity"},
                                      notifications=[notification(value=61.0)]))
        first, second = self.receiver.station_view("station-a")["generations"]
        self.assertEqual(first["observations"][0]["sensor"], "temperature")
        self.assertEqual(second["observations"][0]["sensor"], "humidity")
        self.assertEqual(first["received_sequences"], [1])
        self.assertEqual(second["received_sequences"], [1])

    def test_binding_update_interleaving_resolves_by_sequence_not_arrival(self):
        # 绑定更新与在途通知交错：先到的 seq 3 已换绑定，后到的 seq 1 仍按旧绑定解析。
        self.receiver.ingest(envelope(sequence=3, bindings={"11": "humidity"},
                                      notifications=[notification(value=55.0)]))
        self.receiver.ingest(envelope(sequence=1, bindings={"11": "temperature"},
                                      notifications=[notification(value=12.2)]))
        observations = {obs["sequence"]: obs for obs in self.view()["observations"]}
        self.assertEqual(observations[1]["sensor"], "temperature")
        self.assertEqual(observations[3]["sensor"], "humidity")
        self.assertEqual([gap["sequence"] for gap in self.view()["recoverable_sequences"]], [2])

    def test_unknown_handle_stays_unresolved(self):
        self.receiver.ingest(envelope(notifications=[notification(handle=99)]))
        (observation,) = self.view()["observations"]
        self.assertIsNone(observation["sensor"])
        self.assertFalse(observation["resolved"])

    def test_sequence_wrap_is_continuous(self):
        for sequence in (SEQ_MAX - 1, SEQ_MAX, 1, 2):
            self.receiver.ingest(envelope(sequence=sequence))
        view = self.view()
        self.assertEqual(view["recoverable_sequences"], [])
        self.assertEqual(sorted(view["received_sequences"]), [1, 2, SEQ_MAX - 1, SEQ_MAX])

    def test_gap_across_wrap(self):
        self.receiver.ingest(envelope(sequence=SEQ_MAX))
        self.receiver.ingest(envelope(sequence=2))
        self.assertEqual([gap["sequence"] for gap in self.view()["recoverable_sequences"]], [1])

    def test_absurd_sequence_jump_is_rejected(self):
        self.receiver.ingest(envelope(sequence=1))
        with self.assertRaisesRegex(ValueError, "too far"):
            self.receiver.ingest(envelope(sequence=1 + (1 << 20) + 1))
        self.assertEqual(self.view()["received_sequences"], [1])
        self.assertEqual(len(self.receiver.events("sequence_jump_rejected")), 1)

    def test_queued_ack_survives_reopen(self):
        self.receiver.ingest(envelope(notifications=[notification()]))
        self.receiver.close()
        self.receiver = Receiver(self.path)
        self.assertEqual(len(self.receiver.queued_acks()), 1)
        self.assertEqual(len(self.view()["observations"]), 1)

    def test_republish_attempts_are_bounded(self):
        self.receiver.ingest(envelope(sequence=1))
        self.receiver.ingest(envelope(sequence=3))
        for _ in range(3):
            self.assertEqual(len(self.receiver.pending_gaps(max_attempts=3)), 1)
            self.receiver.record_republish_error("station-a", "g1", 2, "timeout")
        self.assertEqual(self.receiver.pending_gaps(max_attempts=3), [])
        view = self.view()
        self.assertEqual(view["recoverable_sequences"][0]["republish_attempts"], 3)
        self.assertEqual(view["permanent_gaps"], [])

    def test_permanent_gap_survives_later_messages(self):
        self.receiver.ingest(envelope(sequence=1, notifications=[notification(value=1.0)]))
        self.receiver.ingest(envelope(sequence=3, notifications=[notification(value=3.0)]))
        self.receiver.mark_gap_permanent("station-a", "g1", 2, "BadMessageNotAvailable")
        self.receiver.ingest(envelope(sequence=4, notifications=[notification(value=4.0)]))
        view = self.view()
        self.assertEqual([gap["sequence"] for gap in view["permanent_gaps"]], [2])
        self.assertEqual([obs["sequence"] for obs in view["observations"]], [1, 3, 4])

    def test_stations_are_independent(self):
        self.receiver.ingest(envelope(gateway="station-a", sequence=1))
        self.receiver.ingest(envelope(gateway="station-a", sequence=3))
        self.receiver.ingest(envelope(gateway="station-b", sequence=1))
        self.assertEqual(self.receiver.stations(), ["station-a", "station-b"])
        view_a = self.receiver.station_view("station-a")
        view_b = self.receiver.station_view("station-b")
        self.assertEqual([gap["sequence"] for gap in view_a["generations"][0]["recoverable_sequences"]], [2])
        self.assertEqual(view_b["generations"][0]["recoverable_sequences"], [])
        self.assertEqual(len(self.receiver.queued_acks()), 3)

    def test_invalid_envelope_writes_nothing(self):
        with self.assertRaises(ValueError):
            self.receiver.ingest({"gateway": "station-a"})
        self.assertEqual(self.receiver.stations(), [])
        self.assertEqual(self.receiver.queued_acks(), [])

    def test_redelivery_does_not_rewind_retransmission_snapshot(self):
        self.receiver.ingest(envelope(sequence=1, available=[1, 2, 3]))
        self.receiver.ingest(envelope(sequence=3, available=[1, 3]))
        self.receiver.ingest(envelope(sequence=1, available=[1, 2, 3]))
        self.assertEqual(self.view()["retransmission_available"], [1, 3])


class GatewayDrillTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.path = Path(self.temporary.name) / "reception.sqlite3"

    def tearDown(self):
        self.temporary.cleanup()

    def receiver(self):
        return Receiver(self.path)

    def test_full_pass_against_fixture_gateway(self):
        with serve() as origin:
            receiver = self.receiver()
            try:
                summary = subscribe_once(receiver, Client(origin.base_url))
                self.assertEqual(summary["received"], 3)
                self.assertEqual(summary["observations"], 2)
                self.assertEqual(summary["keepalives"], 1)
                self.assertEqual(summary["new_gaps"], [{"gateway": "station-a", "generation": "g1", "sequence": 2}])
                self.assertEqual(summary["republish"]["permanent"], [2])
                self.assertEqual(summary["acks_sent"], 3)
                self.assertEqual(summary["acks_queued"], 0)
                ledger = [(entry["generation"], entry["sequence"]) for entry in origin.acks]
                self.assertEqual(ledger, [("g1", 1), ("g1", 3), ("g2", 1)])
                view = receiver.station_view("station-a")
                first, second = view["generations"]
                self.assertEqual(first["generation"], "g1")
                self.assertEqual([obs["sensor"] for obs in first["observations"]], ["temperature", "temperature"])
                self.assertEqual([obs["quality"] for obs in first["observations"]], ["Good", "Uncertain"])
                self.assertEqual([gap["sequence"] for gap in first["permanent_gaps"]], [2])
                self.assertEqual(first["retransmission_available"], [1, 3])
                self.assertEqual(second["generation"], "g2")
                self.assertEqual(second["keepalive_sequences"], [1])
                self.assertEqual(second["bindings"], {"11": "humidity"})
                self.assertEqual(second["observations"], [])
                self.assertEqual(view["acknowledgements"], {"sent": 3, "queued": 0, "failed": 0})
            finally:
                receiver.close()

    def test_replay_is_idempotent(self):
        with serve() as origin:
            receiver = self.receiver()
            try:
                subscribe_once(receiver, Client(origin.base_url))
                replay = subscribe_once(receiver, Client(origin.base_url))
                self.assertEqual(replay["duplicates"], 3)
                self.assertEqual(replay["observations"], 0)
                self.assertEqual(replay["republish"]["attempted"], 0)
                view = receiver.station_view("station-a")
                self.assertEqual(len(view["generations"][0]["observations"]), 2)
                self.assertEqual(len(origin.acks), 3)
            finally:
                receiver.close()

    def test_republish_success_fills_gap(self):
        message_two = envelope(sequence=2, notifications=[notification(value=12.3, source_time="2026-11-10T10:00:01Z")])
        scenario = {
            "portal": {"csrf": "x"},
            "routes": {
                "/feed": {"json": {"items": [
                    envelope(sequence=1, bindings={"11": "temperature"},
                             notifications=[notification(value=12.2)], available=[1, 2, 3]),
                    envelope(sequence=3, notifications=[notification(value=12.4)], available=[1, 2, 3]),
                ]}},
                "/republish/g1/2": {"json": message_two},
            },
        }
        with serve(scenario) as origin:
            receiver = self.receiver()
            try:
                summary = subscribe_once(receiver, Client(origin.base_url))
                self.assertEqual(summary["republish"]["filled"], [2])
                view = receiver.station_view("station-a")["generations"][0]
                self.assertEqual(view["recoverable_sequences"], [])
                self.assertEqual(view["permanent_gaps"], [])
                self.assertEqual([obs["sequence"] for obs in view["observations"]], [1, 2, 3])
                self.assertEqual(view["observations"][1]["value"], 12.3)
            finally:
                receiver.close()

    def test_ack_failure_stays_queued_for_next_recovery(self):
        with serve() as origin:
            receiver = self.receiver()
            try:
                subscribe_once(receiver, Client(origin.base_url))
            finally:
                receiver.close()
        receiver = self.receiver()
        try:
            dead = Client("http://127.0.0.1:1")
            receiver.ingest(envelope(gateway="station-b", sequence=1, notifications=[notification()]))
            self.assertEqual(flush_acks(receiver, dead), 0)
            self.assertEqual(len(receiver.queued_acks()), 1)
            self.assertEqual(len(receiver.events("ack_send_failed")), 1)
        finally:
            receiver.close()

    def test_envelope_file_without_gateway_leaves_acks_queued(self):
        path = Path(self.temporary.name) / "envelopes.json"
        path.write_text(json.dumps({"items": [
            envelope(sequence=1, bindings={"11": "temperature"}, notifications=[notification()]),
            envelope(sequence=3, notifications=[notification(value=2.0)]),
        ]}), encoding="utf-8")
        receiver = self.receiver()
        try:
            summary = subscribe_once(receiver, envelope_file=path)
            self.assertEqual(summary["received"], 2)
            self.assertEqual(summary["acks_sent"], 0)
            self.assertEqual(summary["acks_queued"], 2)
            self.assertEqual(summary["republish"]["attempted"], 0)
            view = receiver.station_view("station-a")["generations"][0]
            self.assertEqual([gap["sequence"] for gap in view["recoverable_sequences"]], [2])
        finally:
            receiver.close()

    def test_load_envelopes_requires_object_list(self):
        path = Path(self.temporary.name) / "bad.json"
        path.write_text(json.dumps({"items": [1]}), encoding="utf-8")
        with self.assertRaises(ValueError):
            load_envelopes(path)

    def test_drill_runs_end_to_end(self):
        result = drill()
        self.assertEqual(result["first_pass"]["acks_sent"], 3)
        self.assertEqual(result["replay_pass"]["duplicates"], 3)
        self.assertEqual(
            [(entry["generation"], entry["sequence"]) for entry in result["gateway_ack_ledger"]],
            [("g1", 1), ("g1", 3), ("g2", 1)],
        )
        station = result["stations"][0]
        self.assertEqual(station["gateway"], "station-a")
        first = station["generations"][0]
        self.assertEqual([gap["sequence"] for gap in first["permanent_gaps"]], [2])
        self.assertEqual(len(first["observations"]), 2)


if __name__ == "__main__":
    unittest.main()
