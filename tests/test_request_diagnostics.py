"""Fork: a failed request says why (HTTP status, Arlo error fields), and a
stream start that gets no URL says so, without logging URLs or tokens."""

import threading
from unittest import TestCase
from unittest.mock import MagicMock

from pyaarlo.backend import ArloBackEnd, _error_summary
from pyaarlo.camera import ArloCamera


class FakeCfg(object):
    host = "https://arlo.example"
    request_timeout = 60


class FakeArlo(object):
    cfg = FakeCfg()

    def __init__(self):
        self.warnings = []

    def warning(self, msg):
        self.warnings.append(msg)

    def debug(self, msg):
        pass

    def vdebug(self, msg):
        pass


class JsonResponse(object):
    def __init__(self, status_code, body, content_type="application/json"):
        self.status_code = status_code
        self.headers = {"Content-Type": content_type}
        self._body = body
        self.text = str(body)

    def json(self):
        return self._body


class FakeSession(object):
    def __init__(self, response):
        self.response = response

    def post(self, url, json=None, headers=None, timeout=None, cookies=None):
        return self.response


def backend_for(response):
    backend = ArloBackEnd.__new__(ArloBackEnd)
    backend._arlo = FakeArlo()
    backend._req_lock = threading.Lock()
    backend._session = FakeSession(response)
    return backend


SECRET = "secret-egress-token-value"


class TestRequestDiagnostics(TestCase):
    def test_http_error_logs_status_and_arlo_error_without_secrets(self):
        body = {"success": False,
                "data": {"error": "2217", "message": "Device is busy",
                         "reason": "call in progress",
                         "url": "rtsps://x.arlo.com/live?egressToken=" + SECRET}}
        backend = backend_for(JsonResponse(400, body))
        code, data = backend._request_tuple(
            "/hmsweb/users/devices/startStream?token=" + SECRET, method="POST",
            headers={"Authorization": SECRET})
        self.assertEqual((code, data), (400, None))
        self.assertEqual(len(backend._arlo.warnings), 1)
        msg = backend._arlo.warnings[0]
        self.assertIn("POST /hmsweb/users/devices/startStream returned HTTP 400", msg)
        self.assertIn("error=2217", msg)
        self.assertIn("message=Device is busy", msg)
        self.assertIn("reason=call in progress", msg)
        self.assertNotIn(SECRET, msg)
        self.assertNotIn("://", msg)

    def test_401_is_left_to_the_relogin_path(self):
        backend = backend_for(JsonResponse(401, {"success": False}))
        backend._is_logging_in = True  # no re-login attempt in this test
        backend._request_tuple("/x", method="POST")
        self.assertEqual(backend._arlo.warnings, [])

    def test_non_object_body_and_missing_envelope_are_logged(self):
        backend = backend_for(JsonResponse(200, ["unexpected"]))
        self.assertEqual(backend._request_tuple("/a", method="POST"), (500, None))
        self.assertEqual(backend._arlo.warnings, ["request POST /a: response is not a JSON object"])

        backend = backend_for(JsonResponse(200, {"something": 1}))
        self.assertEqual(backend._request_tuple("/b", method="POST"), (500, None))
        self.assertEqual(backend._arlo.warnings,
                         ["request POST /b: unexpected response without success or meta"])

    def test_success_is_unchanged_and_quiet(self):
        backend = backend_for(JsonResponse(200, {"success": True, "data": {"url": "u"}}))
        self.assertEqual(backend._request_tuple("/ok", method="POST"), (200, {"url": "u"}))
        self.assertEqual(backend._arlo.warnings, [])

    def test_error_summary_keeps_only_short_fields(self):
        self.assertEqual(_error_summary(None), "")
        self.assertEqual(_error_summary({"meta": {"code": 403, "error": 9999, "message": "nope"}}),
                         " (error=9999, code=403, message=nope)")
        long = _error_summary({"message": "m" * 200})
        self.assertEqual(long, " (message=" + "m" * 80 + ")")


def camera_with_response(response):
    cam = ArloCamera.__new__(ArloCamera)
    cam._name = "Doorbell"
    cam._lock = threading.Condition()
    cam._stream_lock = threading.Lock()
    cam._user_requests = set()
    cam._local_users = set()
    cam._remote_users = set()
    cam._stream_url = None
    cam._stream_active = False
    cam._attrs = {}
    arlo = MagicMock()
    arlo.be.post.return_value = response
    arlo.be.gen_trans_id.return_value = "t"
    cam._arlo = arlo
    cam._device_id = "dev"
    cam._parent_id = "par"
    cam._unique_id = "uid"
    cam._xcloud_id = "x"
    cam._dump_activities = lambda *_: None
    return cam, arlo


class TestStartStreamWithoutUrl(TestCase):
    def test_no_url_logs_keys_not_values(self):
        cam, arlo = camera_with_response({"something": SECRET})
        url = _start_locked(cam)
        self.assertIsNone(url)
        warning = arlo.warning.call_args[0][0]
        self.assertIn("startUserStream answered without a url (keys=['something'])", warning)
        self.assertNotIn(SECRET, warning)
        self.assertFalse(cam._stream_active)

    def test_failed_post_is_reported(self):
        cam, arlo = camera_with_response(None)
        self.assertIsNone(_start_locked(cam))
        self.assertIn("startUserStream failed", arlo.warning.call_args[0][0])

    def test_url_is_returned_as_before(self):
        cam, arlo = camera_with_response({"url": "rtsp://host.arlo.com/live"})
        self.assertEqual(_start_locked(cam), "rtsps://host.arlo.com/live")
        arlo.warning.assert_not_called()
        self.assertTrue(cam._stream_active)


def _start_locked(cam):
    from unittest.mock import patch
    props = {
        "web_id": "web", "resource_id": "cameras/dev", "device_id": "dev",
        "parent_id": "par", "xcloud_id": "x", "is_taking_idle_snapshot": False,
        "name": "Doorbell",
    }
    patches = [patch.object(ArloCamera, k, new=v) for k, v in props.items()]
    for p in patches:
        p.start()
    try:
        return cam._start_stream_locked("streaming", None)
    finally:
        for p in patches:
            p.stop()


class TestSuccessFalseNamesThePath(TestCase):
    def test_success_false_names_method_and_path_not_values(self):
        body = {"success": False,
                "data": {"error": "2059", "message": "Device is offline.",
                         "reason": "Device is offline.", "url": "https://x/" + SECRET}}
        backend = backend_for(JsonResponse(200, body))
        self.assertEqual(
            backend._request_tuple("/hmsweb/users/devices/notify/BASE123?x=" + SECRET,
                                   method="POST"), (500, None))
        self.assertEqual(backend._arlo.warnings, [
            "error in response to POST /hmsweb/users/devices/notify/BASE123"
            " (error=2059, message=Device is offline., reason=Device is offline.)"])
