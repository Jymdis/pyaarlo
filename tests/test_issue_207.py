"""Fork regression tests for twrecked/pyaarlo issue #207.

A URL lookup (action "get") must not count as a running stream: the next
start has to POST startUserStream, and takeSnapshot is only used while a
stream is confirmed active.
"""

from unittest.mock import MagicMock

import pytest

from pyaarlo.camera import ArloCamera
from pyaarlo.constant import (
    IDLE_SNAPSHOT_PATH,
    STREAM_SNAPSHOT_PATH,
    STREAM_START_PATH,
)
from pyaarlo.storage import ArloStorage


@pytest.fixture
def camera(monkeypatch):
    # No base station in this synthetic setup; takeSnapshot reads a timezone.
    monkeypatch.setattr(ArloCamera, "timezone", property(lambda self: "UTC"))
    arlo = MagicMock()
    arlo.cfg.state_file = None
    arlo.cfg.library_days = 7
    arlo.cfg.stream_snapshot = False
    arlo.cfg.stream_snapshot_stop = 0
    arlo.cfg.snapshot_checks = []
    arlo.st = ArloStorage(arlo)
    return ArloCamera("Synthetic doorbell", arlo, {
        "deviceId": "test-device", "deviceType": "doorbell",
        "parentId": "test-base", "userId": "test-user", "modelId": "TEST",
    })


def _start_bodies(camera):
    return [c.args[1] for c in camera._arlo.be.post.call_args_list
            if c.args and c.args[0] == STREAM_START_PATH]


def _bg_paths(camera):
    return [c.kwargs.get("path") for c in camera._arlo.bg.run.call_args_list]


def test_url_lookup_does_not_register_a_local_user(camera):
    camera._arlo.be.post.return_value = {"url": "rtsp://example.invalid/lookup"}
    assert camera.get_stream_url() == "rtsps://example.invalid/lookup"
    assert not camera.has_any_local_users
    assert not camera.is_stream_active


def test_start_after_lookup_posts_start_user_stream(camera):
    camera._arlo.be.post.side_effect = [
        {"url": "rtsp://example.invalid/lookup"},
        {"url": "rtsp://example.invalid/live"},
    ]
    camera.get_stream_url()
    assert camera.start_snapshot_stream() == "rtsps://example.invalid/live"
    bodies = _start_bodies(camera)
    assert [b["action"] for b in bodies] == ["get", "set"]
    assert bodies[1]["properties"]["activityState"] == "startUserStream"
    assert camera.is_stream_active


def test_stale_local_users_do_not_skip_start_user_stream(camera):
    # Left over from an earlier session that Arlo has already ended.
    camera._local_users.add("streaming")
    camera._stream_url = "rtsps://example.invalid/stale"
    camera._arlo.be.post.return_value = {"url": "rtsp://example.invalid/fresh"}
    assert camera.get_stream() == "rtsps://example.invalid/fresh"
    assert camera._arlo.be.post.call_count == 1


def test_snapshot_after_lookup_is_an_idle_snapshot(camera):
    camera._arlo.be.post.return_value = {"url": "rtsp://example.invalid/lookup"}
    camera.get_stream_url()
    camera.request_snapshot()
    assert _bg_paths(camera) == [IDLE_SNAPSHOT_PATH]


def test_snapshot_uses_take_snapshot_when_stream_is_active(camera):
    camera._event_handler("cameras/test-device", {
        "properties": {"activityState": "userStreamActive"}})
    assert camera.is_stream_active
    camera.request_snapshot()
    assert _bg_paths(camera) == [STREAM_SNAPSHOT_PATH]


def test_doorbell_alert_stream_counts_as_active(camera):
    camera._event_handler("cameras/test-device", {
        "properties": {"activityState": "alertStreamActive"}})
    camera.request_snapshot()
    assert _bg_paths(camera) == [STREAM_SNAPSHOT_PATH]


def test_stream_snapshot_config_starts_a_stream_first(camera):
    camera._arlo.cfg.stream_snapshot = True
    camera._arlo.be.post.return_value = {"url": "rtsp://example.invalid/live"}
    camera.request_snapshot()
    bodies = _start_bodies(camera)
    assert len(bodies) == 1
    assert bodies[0]["properties"]["activityState"] == "startUserStream"
    assert camera.has_local_user("snapshot")
    assert _bg_paths(camera) == [STREAM_SNAPSHOT_PATH]


def test_stream_snapshot_falls_back_to_idle_when_start_fails(camera):
    camera._arlo.cfg.stream_snapshot = True
    camera._arlo.be.post.return_value = None
    camera.request_snapshot()
    assert _bg_paths(camera) == [IDLE_SNAPSHOT_PATH]


def test_idle_event_clears_active_stream(camera):
    camera._arlo.be.post.return_value = {"url": "rtsp://example.invalid/live"}
    camera.get_stream()
    assert camera.is_stream_active
    camera._event_handler("cameras/test-device", {
        "properties": {"activityState": "idle"}})
    assert not camera.is_stream_active
    assert not camera.has_any_local_users
