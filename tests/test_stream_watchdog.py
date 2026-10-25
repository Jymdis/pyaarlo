"""Event stream restart after re-login, the stream watchdog and the
scheduled reconnect interval (0.8.0.27.post2)."""

import threading
import time
from unittest import TestCase
from unittest.mock import MagicMock, patch

import paho.mqtt.client as mqtt

from pyaarlo import backend as backend_mod
from pyaarlo.backend import ArloBackEnd
from tests.test_backend_reauth import FakeArlo


class Backend(TestCase):
    def setUp(self):
        b = ArloBackEnd.__new__(ArloBackEnd)
        b._arlo = FakeArlo()
        b._arlo.cfg.reconnect_every = 0
        b._arlo.cfg.stream_watchdog = 15 * 60
        b._lock = threading.Condition()
        b._login_lock = threading.RLock()
        b._last_login_time = 0
        b._is_logging_in = False
        b._reconnect_job = None
        b._watchdog_job = None
        b._mqtt_reconnect_attempts = 0
        b._user_id = "user123"
        b._token = "token-1"
        b._event_token = "token-1"
        b._use_mqtt = True
        b._stop_thread = False
        b._event_client = MagicMock()
        b._event_connected = True
        b._event_restart_reason = None
        b._event_restart_relogin = True
        b._event_disconnected_at = None
        b._last_event_time = time.monotonic()
        b._watchdog_idle_restarts = 0
        b._expires_in = None
        b._dump_file = None
        self.b = b


class TestTokenExpiry(Backend):
    def test_epoch_seconds(self):
        self.b._expires_in = int(time.time()) + 3600
        self.assertAlmostEqual(self.b._token_seconds_left(), 3600, delta=5)

    def test_epoch_milliseconds(self):
        self.b._expires_in = int(time.time() * 1000) + 3600 * 1000
        self.assertAlmostEqual(self.b._token_seconds_left(), 3600, delta=5)

    def test_duration(self):
        self.b._expires_in = 7200
        self.assertEqual(self.b._token_seconds_left(), 7200)

    def test_unknown(self):
        self.b._expires_in = 0
        self.assertIsNone(self.b._token_seconds_left())


class TestStartMonitoring(Backend):
    def _start(self):
        b = self.b
        b._select_backend = MagicMock()
        b._event_main = MagicMock()

        def fake_start():
            b._event_connected = True

        thread = MagicMock()
        thread.start = fake_start
        with patch("threading.Thread", return_value=thread):
            b.start_monitoring()
        return {job["cb"]: job["seconds"] for job in b._arlo.bg.jobs}

    def test_epoch_expiry_no_longer_disables_reconnect(self):
        # The live system logged "automatically reconnecting every 1792957343s".
        self.b._expires_in = 1792957643
        jobs = self._start()
        self.assertEqual(jobs[self.b._event_reconnect], 90 * 60)

    def test_reconnects_before_token_expiry(self):
        self.b._expires_in = int(time.time()) + 20 * 60
        jobs = self._start()
        self.assertAlmostEqual(jobs[self.b._event_reconnect], 15 * 60, delta=5)

    def test_never_more_often_than_ten_minutes(self):
        self.b._expires_in = int(time.time()) + 60
        jobs = self._start()
        self.assertEqual(jobs[self.b._event_reconnect], 600)

    def test_unknown_expiry_uses_default(self):
        jobs = self._start()
        self.assertEqual(jobs[self.b._event_reconnect], 90 * 60)

    def test_watchdog_scheduled_and_cancelled(self):
        jobs = self._start()
        self.assertEqual(jobs[self.b._event_watchdog], backend_mod.WATCHDOG_INTERVAL)
        self.b._event_client = None
        self.b._event_thread = None
        self.b.stop()
        self.assertEqual(self.b._arlo.bg.jobs, [])


class TestReloginRestartsStream(Backend):
    def test_relogin_restarts_stream_without_second_login(self):
        b = self.b
        client = b._event_client

        def fake_login():
            b._token = "token-2"
            b._last_login_time = time.monotonic()
            return True

        b._login = fake_login
        self.assertTrue(b._relogin())
        client.disconnect.assert_called_once()
        self.assertEqual(b._event_restart_reason, "token refreshed by re-login")
        self.assertFalse(b._event_restart_relogin)
        self.assertTrue(any("token refreshed by re-login" in w for w in b._arlo.warnings))

    def test_failed_relogin_leaves_stream(self):
        b = self.b
        b._login = lambda: False
        self.assertFalse(b._relogin())
        b._event_client.disconnect.assert_not_called()
        self.assertIsNone(b._event_restart_reason)

    def test_connect_with_replaced_token_restarts(self):
        b = self.b
        b._arlo.bg.run = MagicMock()
        b._mqtt_subscribe = MagicMock()
        b._token = "token-2"  # re-login happened while connecting with token-1
        b._mqtt_on_connect(b._event_client, None, {}, 0)
        self.assertIsNotNone(b._event_restart_reason)
        self.assertFalse(b._event_restart_relogin)

    def test_rejected_connect_asks_for_login(self):
        b = self.b
        b._mqtt_on_connect(b._event_client, None, {}, 5)
        self.assertTrue(b._event_restart_relogin)
        self.assertIn("rejected", b._event_restart_reason)


class TestWatchdog(Backend):
    def test_healthy_stream_left_alone(self):
        self.b._event_watchdog()
        self.assertIsNone(self.b._event_restart_reason)
        self.b._event_client.disconnect.assert_not_called()
        self.assertEqual(self.b._arlo.warnings, [])

    def test_idle_stream_restarted_without_login(self):
        b = self.b
        b._last_event_time = time.monotonic() - 16 * 60
        b._event_watchdog()
        b._event_client.disconnect.assert_called_once()
        self.assertIn("no event stream traffic for 16 minutes", b._event_restart_reason)
        self.assertFalse(b._event_restart_relogin)
        self.assertEqual(len(b._arlo.warnings), 1)

    def test_idle_limit_backs_off_and_traffic_resets_it(self):
        b = self.b
        b._last_event_time = time.monotonic() - 16 * 60
        b._event_watchdog()
        self.assertEqual(b._watchdog_idle_restarts, 1)
        # 20 minutes of silence after a restart is under the doubled limit.
        b._event_restart_reason = None
        b._last_event_time = time.monotonic() - 20 * 60
        b._event_watchdog()
        self.assertIsNone(b._event_restart_reason)
        b._last_event_time = time.monotonic() - 31 * 60
        b._event_watchdog()
        self.assertIsNotNone(b._event_restart_reason)
        self.assertEqual(b._watchdog_idle_restarts, 2)
        # Any message resets the back-off.
        msg = MagicMock(topic="u/user123/in/x", payload=b"{}")
        b._event_handle_response = MagicMock()
        b._mqtt_on_message(None, None, msg)
        self.assertEqual(b._watchdog_idle_restarts, 0)

    def test_idle_check_can_be_turned_off(self):
        b = self.b
        b._arlo.cfg.stream_watchdog = 0
        b._last_event_time = time.monotonic() - 24 * 3600
        b._event_watchdog()
        self.assertIsNone(b._event_restart_reason)

    def test_stale_token_restarted(self):
        b = self.b
        b._token = "token-2"
        b._event_watchdog()
        self.assertIn("token from before the last re-login", b._event_restart_reason)
        self.assertFalse(b._event_restart_relogin)

    def test_disconnected_too_long_restarted(self):
        b = self.b
        b._event_disconnected_at = time.monotonic() - 30
        b._event_watchdog()
        self.assertIsNone(b._event_restart_reason)
        b._event_disconnected_at = time.monotonic() - 120
        b._event_watchdog()
        self.assertIn("disconnected for", b._event_restart_reason)

    def test_disconnect_and_connect_tracked(self):
        b = self.b
        b._arlo.bg.run = MagicMock()
        b._mqtt_subscribe = MagicMock()
        b._mqtt_on_disconnect(None, None, 7)
        self.assertIsNotNone(b._event_disconnected_at)
        b._mqtt_on_connect(b._event_client, None, {}, 0)
        self.assertIsNone(b._event_disconnected_at)

    def test_sse_idle_stream_stopped(self):
        b = self.b
        b._use_mqtt = False
        b._last_event_time = time.monotonic() - 16 * 60
        b._event_watchdog()
        b._event_client.stop.assert_called_once()


class TestMqttLoop(Backend):
    def test_restart_request_ends_loop(self):
        b = self.b
        client = MagicMock()

        def loop(timeout):
            b._event_restart_reason = "test"
            return mqtt.MQTT_ERR_SUCCESS

        client.loop.side_effect = loop
        b._mqtt_loop(client)
        client.disconnect.assert_called_once()
        client.reconnect.assert_not_called()

    def test_reconnect_uses_current_token(self):
        b = self.b
        b._lock = MagicMock()
        client = MagicMock()
        b._token = "token-2"
        calls = []

        def loop(timeout):
            calls.append(1)
            if len(calls) == 1:
                return mqtt.MQTT_ERR_CONN_LOST
            b._stop_thread = True
            return mqtt.MQTT_ERR_SUCCESS

        client.loop.side_effect = loop
        b._mqtt_loop(client)
        client.username_pw_set.assert_called_once_with("user123", "token-2")
        client.reconnect.assert_called_once()
        self.assertEqual(b._event_token, "token-2")


class TestEventMainLogin(Backend):
    def _one_pass(self, relogin):
        b = self.b
        b._logged_in = True

        def fake_main():
            b._event_restart_reason = "test"
            b._event_restart_relogin = relogin
            b._stop_thread = True

        b._mqtt_main = fake_main
        b._event_main()

    def test_watchdog_restart_keeps_login(self):
        self._one_pass(relogin=False)
        self.assertTrue(self.b._logged_in)
        self.assertIsNone(self.b._event_restart_reason)
        self.assertTrue(self.b._event_restart_relogin)

    def test_other_restarts_log_in_again(self):
        self._one_pass(relogin=True)
        self.assertFalse(self.b._logged_in)


class TestSubscribe(Backend):
    def test_every_connect_subscribes_user_and_device_topics(self):
        b = self.b
        b._arlo.devices = [{"allowedMqttTopics": ["d/X1/out/#"]},
                           {"allowedMqttTopics": ["d/X1/out/#", "d/X2/out/#"]}]
        b._arlo.bg.run = MagicMock()
        for _ in range(2):  # first connect and a reconnect
            b._mqtt_on_connect(b._event_client, None, {}, 0)
        expected = [("u/user123/in/#", 0), ("d/X1/out/#", 0), ("d/X2/out/#", 0)]
        self.assertEqual(b._event_client.subscribe.call_count, 2)
        b._event_client.subscribe.assert_called_with(expected)
        self.assertEqual(b._arlo.warnings, [])

    def test_no_device_topics_is_reported(self):
        b = self.b
        b._arlo.devices = [{"deviceId": "x"}]
        b._mqtt_subscribe()
        b._event_client.subscribe.assert_called_once_with([("u/user123/in/#", 0)])
        self.assertTrue(any("allowedMqttTopics" in w for w in b._arlo.warnings))

    def test_refused_subscription_is_reported(self):
        b = self.b
        b._event_topics = [("u/user123/in/#", 0), ("d/X1/out/#", 0)]
        b._mqtt_on_subscribe(None, None, 1, (0, 128))
        self.assertEqual(len(b._arlo.warnings), 1)
        self.assertIn("d/X1/out/#", b._arlo.warnings[0])
        b._mqtt_on_subscribe(None, None, 2, (0, 0))
        self.assertEqual(len(b._arlo.warnings), 1)
