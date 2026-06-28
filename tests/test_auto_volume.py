"""Tests for AutoVolume (AV) support."""
import pytest

from pybosecsp.client import BoseCSPDevice


@pytest.fixture
def device():
    dev = BoseCSPDevice("1.2.3.4", ["Bar"])
    return dev


def _capture_sends(dev):
    sent = []

    async def fake_send(cmd):
        sent.append(cmd)

    dev._send_command = fake_send
    return sent


def test_parse_av_on_off(device):
    device._parse_response('GA "Bar AV">1=2')
    assert device.get_zone_state("Bar").auto_volume is True
    device._parse_response('GA "Bar AV">1=1')
    assert device.get_zone_state("Bar").auto_volume is False


@pytest.mark.asyncio
async def test_set_volume_noop_when_av_on(device):
    sent = _capture_sends(device)
    device._parse_response('GA "Bar AV">1=2')  # AV On
    await device.set_volume("Bar", -10.0)
    assert sent == []  # no doomed gain command
    assert device.get_zone_state("Bar").volume == 0.0  # no phantom update


@pytest.mark.asyncio
async def test_set_volume_works_when_av_off(device):
    sent = _capture_sends(device)
    device._parse_response('GA "Bar AV">1=1')  # AV Off
    await device.set_volume("Bar", -10.0)
    assert sent == ['SA "Bar Gain">1=-10.0']


@pytest.mark.asyncio
async def test_set_auto_volume_commands(device):
    sent = _capture_sends(device)
    await device.set_auto_volume("Bar", True)
    assert sent == ['SA "Bar AV">1=2']
    sent.clear()
    await device.set_auto_volume("Bar", False)
    assert sent == ['SA "Bar AV">1=1']


@pytest.mark.asyncio
async def test_query_auto_volume(device):
    sent = _capture_sends(device)
    await device.query_auto_volume("Bar")
    assert sent == ['GA "Bar AV">1']
