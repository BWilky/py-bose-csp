"""Tests for the reconnect retry loop.

Regression coverage for the bug where a single failed reconnect attempt (e.g.
the CSP web configuration dashboard holding an exclusive session and refusing
the SoIP port) killed the retry chain, leaving the integration disconnected
until a manual reload.
"""
import asyncio

import pytest

from pybosecsp.client import BoseCSPDevice


@pytest.mark.asyncio
async def test_handle_reconnect_retries_until_success():
    """_handle_reconnect keeps retrying after failed connect() attempts."""
    dev = BoseCSPDevice("1.2.3.4", ["Bar"], reconnect_delay=0)
    dev._running = True

    attempts = 0
    fail_times = 3

    async def fake_connect():
        nonlocal attempts
        attempts += 1
        if attempts <= fail_times:
            raise Exception("ECONNREFUSED (dashboard open)")
        dev.is_connected = True  # success on the 4th attempt

    dev.connect = fake_connect
    await asyncio.wait_for(dev._handle_reconnect(), timeout=5)

    assert dev.is_connected
    assert attempts == fail_times + 1


@pytest.mark.asyncio
async def test_handle_reconnect_no_attempts_when_stopped():
    """_handle_reconnect does nothing once the client has been stopped."""
    dev = BoseCSPDevice("1.2.3.4", ["Bar"], reconnect_delay=0)
    dev._running = False

    attempts = 0

    async def fake_connect():
        nonlocal attempts
        attempts += 1

    dev.connect = fake_connect
    await asyncio.wait_for(dev._handle_reconnect(), timeout=5)

    assert attempts == 0


@pytest.mark.asyncio
async def test_handle_reconnect_stops_after_success():
    """The loop exits as soon as a connect() succeeds (no extra attempts)."""
    dev = BoseCSPDevice("1.2.3.4", ["Bar"], reconnect_delay=0)
    dev._running = True

    attempts = 0

    async def fake_connect():
        nonlocal attempts
        attempts += 1
        dev.is_connected = True

    dev.connect = fake_connect
    await asyncio.wait_for(dev._handle_reconnect(), timeout=5)

    assert attempts == 1
    assert dev.is_connected
