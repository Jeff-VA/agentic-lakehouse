"""Playback sessions for the example's `telemetry` dataset: a stand-in for real
client logs (ingestion.yaml names it as the dataset's `simulator`).

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


def session(rng: random.Random, start: datetime) -> list[dict]:
    """One viewer session: a stream start event, then heartbeats every 20-40 seconds."""
    session_id = f"{rng.getrandbits(64):016x}"
    isp = rng.choices(list(ISPS), [share for share, _, _ in ISPS.values()])[0]
    device = rng.choices(list(DEVICES), [share for share, _ in DEVICES.values()])[0]
    _, median_ttfb, rebuffer_chance = ISPS[isp]
    latency_scale = median_ttfb * (1.3 if device in MOBILE else 1.0)
    events, ts = [], start
    for i in range(1 + int(rng.expovariate(1 / 8))):
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
    return events


def _buffer_duration(rng: random.Random, rebuffering: bool) -> int:
    if rebuffering:
        return 201 + round(rng.expovariate(1 / 600))  # a stall the viewer notices
    return round(rng.uniform(0, 200)) if rng.random() < 0.15 else 0  # a brief dip, or none
