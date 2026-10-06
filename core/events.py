"""Synthetic playback telemetry: a stand-in for real client logs.

Generates sessions of playback events matching the `telemetry` dataset in
config/ingestion.yaml. Output is fully determined by the window and seed, so
the same batch can be loaded into every tier and the results compared.

Network quality varies by ISP, and `cox` is deliberately degraded (slower
first byte, more rebuffering), so there is a real finding to discover.
"""

import math
import random
from datetime import datetime, timedelta

# ISP -> (share of sessions, median time-to-first-byte in ms, chance an event rebuffers)
ISPS = {
    "comcast": (0.30, 110, 0.03),
    "verizon": (0.22, 95, 0.02),
    "att": (0.18, 120, 0.04),
    "spectrum": (0.15, 130, 0.04),
    "cox": (0.10, 210, 0.11),
    "frontier": (0.05, 150, 0.05),
}
# Device -> (share of sessions, typical bitrate in kbps)
DEVICES = {
    "ios": (0.25, 2500),
    "android": (0.25, 2200),
    "web": (0.20, 5000),
    "smart_tv": (0.22, 12000),
    "console": (0.08, 10000),
}
MOBILE = {"ios", "android"}


def generate_events(start: datetime, end: datetime, *, sessions_per_minute: float = 6, seed: int = 0) -> list[dict]:
    """Events in [start, end) as dicts keyed by column, sorted by event_ts.

    `start` and `end` are naive UTC datetimes. Each session opens with a stream
    start event and then reports heartbeats roughly every 30 seconds.
    """
    rng = random.Random(seed)
    isps, isp_weights = list(ISPS), [share for share, _, _ in ISPS.values()]
    devices, device_weights = list(DEVICES), [share for share, _ in DEVICES.values()]
    span_ms = int((end - start) / timedelta(milliseconds=1))
    n_sessions = round(span_ms / 60_000 * sessions_per_minute)

    events = []
    for _ in range(n_sessions):
        session_id = f"{rng.getrandbits(64):016x}"
        isp = rng.choices(isps, isp_weights)[0]
        device = rng.choices(devices, device_weights)[0]
        _, median_ttfb, rebuffer_chance = ISPS[isp]
        latency_scale = median_ttfb * (1.3 if device in MOBILE else 1.0)
        ts = start + timedelta(milliseconds=rng.randrange(span_ms))
        for i in range(1 + int(rng.expovariate(1 / 8))):
            if ts >= end:
                break
            stream_start = i == 0
            rebuffering = rng.random() < rebuffer_chance
            # Fetching the first segment of a stream is slower than steady-state requests.
            latency = rng.lognormvariate(math.log(latency_scale * (1.6 if stream_start else 1.0)), 0.45)
            bitrate = rng.gauss(DEVICES[device][1], DEVICES[device][1] * 0.15) * (0.6 if rebuffering else 1.0)
            events.append(
                {
                    "event_ts": ts,
                    "session_id": session_id,
                    "client_isp": isp,
                    "client_device": device,
                    "request_latency_ms": max(1, round(latency)),
                    "stream_start_event": stream_start,
                    "buffer_duration_ms": _buffer_duration(rng, rebuffering),
                    "bitrate_kbps": max(300, round(bitrate)),
                }
            )
            ts += timedelta(milliseconds=rng.randrange(20_000, 40_000))
    events.sort(key=lambda e: e["event_ts"])
    return events


def _buffer_duration(rng: random.Random, rebuffering: bool) -> int:
    if rebuffering:
        return 201 + round(rng.expovariate(1 / 600))  # a stall the viewer notices
    return round(rng.uniform(0, 200)) if rng.random() < 0.15 else 0  # a brief dip, or none
