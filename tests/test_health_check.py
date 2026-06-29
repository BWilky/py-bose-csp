"""Tests for the active Health Checking probe."""
import pytest

import pybosecsp.client as c
from pybosecsp.client import BoseCSPDevice


@pytest.fixture(autouse=True)
def _no_wait(monkeypatch):
    # Skip the real 10s post-nudge wait.
    monkeypatch.setattr(c, "HEALTH_WAIT", 0)


def _make_device(applies_writes=True, external_value=None):
    dev = BoseCSPDevice("1.2.3.4", ["Bar"], zone_limits={"Bar": (-60.0, 12.0)})
    dev._state["Bar"].volume = -10.0
    dev._health_zone = "Bar"
    dev.is_connected = True
    sent = []
    device_vol = {"v": -10.0}

    async def fake_send(cmd):
        sent.append(cmd)
        if cmd.startswith('SA "Bar Gain">1='):
            if applies_writes:
                device_vol["v"] = float(cmd.split("=")[-1])
        elif cmd.startswith('GA "Bar Gain">1'):
            reported = external_value if external_value is not None else device_vol["v"]
            dev._parse_response('GA "Bar Gain">1=%.1f' % reported)

    dev._send_command = fake_send
    return dev, sent


@pytest.mark.asyncio
async def test_probe_pass_restores_and_is_silent():
    dev, sent = _make_device(applies_writes=True)
    calls = []
    dev.subscribe_updates(lambda z: calls.append(z))

    assert await dev._run_single_check() == "pass"
    assert dev._state["Bar"].volume == -10.0  # public state untouched
    assert sent[-1] == 'SA "Bar Gain">1=-10.0'  # restored
    assert calls == []  # no HA-facing update fired


@pytest.mark.asyncio
async def test_probe_fail_restores():
    dev, sent = _make_device(applies_writes=False)
    assert await dev._run_single_check() == "fail"
    assert sent[-1] == 'SA "Bar Gain">1=-10.0'


@pytest.mark.asyncio
async def test_probe_inconclusive_external_change_no_restore():
    dev, sent = _make_device(applies_writes=True, external_value=3.0)
    assert await dev._run_single_check() == "inconclusive"
    assert not any(s == 'SA "Bar Gain">1=-10.0' for s in sent[1:])


@pytest.mark.asyncio
async def test_probe_inconclusive_user_change_flag():
    dev, sent = _make_device(applies_writes=True)
    inner = dev._send_command

    async def send_then_user(cmd):
        await inner(cmd)
        if cmd.startswith('SA "Bar Gain">1=') and dev._probe_zone == "Bar":
            dev._health_user_change = True

    dev._send_command = send_then_user
    assert await dev._run_single_check() == "inconclusive"


def test_zone_selection_skips_auto_volume():
    dev = BoseCSPDevice(
        "1.2.3.4", ["Bar", "Patio"],
        zone_limits={"Bar": (-60, 12), "Patio": (-60, 12)},
    )
    dev._state["Bar"].auto_volume = True
    assert dev._select_health_zone() == "Patio"
    dev._state["Patio"].auto_volume = True
    assert dev._select_health_zone() is None


def test_nudge_respects_floor_and_ceiling():
    dev = BoseCSPDevice("1.2.3.4", ["Bar"], zone_limits={"Bar": (-60.0, 12.0)})
    assert dev._nudge_target("Bar", 12.0) == 11.5
    assert dev._nudge_target("Bar", -60.0) == -59.5
    assert dev._nudge_target("Bar", 0.0) == 0.5


def test_health_status_follows_connection_edges():
    dev = BoseCSPDevice("1.2.3.4", ["Bar"], zone_limits={"Bar": (-60.0, 12.0)})
    statuses = []
    dev.subscribe_health(lambda s: statuses.append(s))  # fires once with current
    dev._health_zone = "Bar"

    dev._notify_availability(True)  # connect edge -> starting
    dev._notify_availability(False)  # disconnect edge -> socket not connected
    assert dev.health_status == c.HEALTH_SOCKET_NOT_CONNECTED

    # Sustained reconnect failures escalate to the terminal state.
    dev._notify_health(c.HEALTH_CANT_RECONNECT)
    # A redundant disconnect edge (already disconnected) must not downgrade it.
    dev._notify_availability(False)
    assert dev.health_status == c.HEALTH_CANT_RECONNECT

    # A real reconnect clears it.
    dev._notify_availability(True)
    assert dev.health_status == c.HEALTH_STARTING
