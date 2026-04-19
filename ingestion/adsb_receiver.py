"""
ingestion/adsb_receiver.py
──────────────────────────
Multi-source ADS-B ingestor. Pulls from THREE free, high-coverage APIs
simultaneously and merges them, giving full global coverage with
8,000–18,000+ aircraft at any time.

Sources (in order of coverage):
  1. adsb.lol      — ADS-B Exchange community feed. No auth. ~15,000+ aircraft.
  2. adsb.fi       — Finnish ADS-B network. No auth. ~12,000+ aircraft.
  3. airplanes.live — Community aggregator. No auth. ~10,000+ aircraft.
  4. OpenSky        — Fallback. Requires no auth but rate-limited.

All sources are polled concurrently. Results are deduplicated by ICAO24.
The merged set is published to Kafka topic: raw.adsb
"""

from __future__ import annotations

import asyncio
import time
import logging
from typing import Optional, AsyncIterator, Dict, List
from dataclasses import dataclass

import aiohttp
from aiokafka import AIOKafkaProducer

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from models import RawADSBMessage
from config import settings

log = logging.getLogger(__name__)


# ─── Kafka ─────────────────────────────────────────────────────────────────────

async def make_producer() -> AIOKafkaProducer:
    producer = AIOKafkaProducer(
        bootstrap_servers=settings.KAFKA_BOOTSTRAP,
        value_serializer=lambda v: v,
        compression_type="lz4",
        linger_ms=10,
        acks="all",
    )
    await producer.start()
    return producer

async def publish(producer: AIOKafkaProducer, msg: RawADSBMessage) -> None:
    await producer.send(
        topic=settings.TOPIC_RAW_ADSB,
        key=msg.icao24.encode(),
        value=msg.to_bytes(),
    )


# ─── Shared HTTP session ───────────────────────────────────────────────────────

HEADERS = {
    "User-Agent": "SkySecure/2.0 (airspace intelligence research)",
    "Accept":     "application/json",
}


# ─── Source 1: adsb.lol (ADS-B Exchange community) ────────────────────────────
# Full global feed. Returns all currently broadcasting aircraft.
# Docs: https://api.adsb.lol/

class ADSBLolSource:
    URL      = "https://api.adsb.lol/v2/all"
    INTERVAL = 8   # seconds between polls

    def __init__(self) -> None:
        self.name = "adsb.lol"

    async def fetch(self, session: aiohttp.ClientSession) -> List[RawADSBMessage]:
        try:
            async with session.get(
                self.URL, headers=HEADERS,
                timeout=aiohttp.ClientTimeout(total=20)
            ) as resp:
                if resp.status != 200:
                    log.warning("[adsb.lol] HTTP %d", resp.status)
                    return []
                data = await resp.json(content_type=None)
                aircraft = data.get("ac") or data.get("aircraft") or []
                now = time.time()
                results = []
                for ac in aircraft:
                    msg = self._parse(ac, now)
                    if msg:
                        results.append(msg)
                log.info("[adsb.lol] %d aircraft", len(results))
                return results
        except Exception as e:
            log.error("[adsb.lol] fetch error: %s", e)
            return []

    def _parse(self, ac: dict, now: float) -> Optional[RawADSBMessage]:
        icao = (ac.get("hex") or ac.get("icao") or "").upper().strip()
        if not icao or len(icao) != 6:
            return None

        # adsb.lol uses "t" for type, alt_baro/alt_geom in feet
        lat = ac.get("lat")
        lon = ac.get("lon")
        if lat is None or lon is None:
            return None
        try:
            lat = float(lat)
            lon = float(lon)
        except (ValueError, TypeError):
            return None

        # Altitude: prefer geometric, fall back to baro
        alt_baro = ac.get("alt_baro") or ac.get("altitude")
        alt_geo  = ac.get("alt_geom")
        try:
            alt_baro = int(alt_baro) if alt_baro not in (None, "ground", "grnd") else 0
        except (ValueError, TypeError):
            alt_baro = None
        try:
            alt_geo = int(alt_geo) if alt_geo is not None else None
        except (ValueError, TypeError):
            alt_geo = None

        gs     = ac.get("gs") or ac.get("spd")          # ground speed knots
        track  = ac.get("track") or ac.get("trk")        # heading
        baro_r = ac.get("baro_rate") or ac.get("vsi")    # vertical rate fpm
        cs     = (ac.get("flight") or ac.get("callsign") or "").strip() or None
        sqk    = ac.get("squawk") or ac.get("sqk")

        try: gs     = float(gs) if gs is not None else None
        except: gs = None
        try: track  = float(track) if track is not None else None
        except: track = None
        try: baro_r = int(baro_r) if baro_r is not None else None
        except: baro_r = None

        return RawADSBMessage(
            receiver_id   = "adsb-lol",
            recv_time     = float(ac.get("seen_pos", now) or now),
            icao24        = icao,
            raw_message   = "",
            msg_type      = 17,
            callsign      = cs,
            lat           = lat,
            lon           = lon,
            altitude_baro = alt_baro,
            altitude_geo  = alt_geo,
            velocity      = gs,
            heading       = track,
            vertical_rate = baro_r,
            on_ground     = (ac.get("alt_baro") in ("ground", "grnd") or ac.get("gnd") is True),
            squawk        = str(sqk) if sqk else None,
            nic           = ac.get("nic"),
            nac_p         = ac.get("nac_p"),
        )


# ─── Source 2: adsb.fi ────────────────────────────────────────────────────────
# Finnish ADS-B aggregator. Free, no auth, excellent European + global coverage.
# Docs: https://api.adsb.fi/

class ADSBFiSource:
    URL      = "https://api.adsb.fi/v1/aircraft"
    INTERVAL = 10

    def __init__(self) -> None:
        self.name = "adsb.fi"

    async def fetch(self, session: aiohttp.ClientSession) -> List[RawADSBMessage]:
        try:
            async with session.get(
                self.URL, headers=HEADERS,
                timeout=aiohttp.ClientTimeout(total=20)
            ) as resp:
                if resp.status != 200:
                    log.warning("[adsb.fi] HTTP %d", resp.status)
                    return []
                data = await resp.json(content_type=None)
                aircraft = data.get("aircraft") or data.get("ac") or []
                now = time.time()
                results = []
                for ac in aircraft:
                    msg = self._parse(ac, now)
                    if msg:
                        results.append(msg)
                log.info("[adsb.fi] %d aircraft", len(results))
                return results
        except Exception as e:
            log.error("[adsb.fi] fetch error: %s", e)
            return []

    def _parse(self, ac: dict, now: float) -> Optional[RawADSBMessage]:
        icao = (ac.get("icao") or ac.get("hex") or "").upper().strip()
        if not icao or len(icao) != 6:
            return None

        lat = ac.get("lat")
        lon = ac.get("lon") or ac.get("lng")
        if lat is None or lon is None:
            return None
        try:
            lat, lon = float(lat), float(lon)
        except (ValueError, TypeError):
            return None

        alt_baro = ac.get("alt_baro") or ac.get("altitude")
        alt_geo  = ac.get("alt_geom")
        try:
            alt_baro = int(alt_baro) if alt_baro not in (None, "ground") else 0
        except: alt_baro = None
        try:
            alt_geo  = int(alt_geo) if alt_geo is not None else None
        except: alt_geo = None

        gs    = ac.get("gs") or ac.get("speed")
        track = ac.get("track") or ac.get("heading")
        vr    = ac.get("baro_rate") or ac.get("vertical_rate")
        cs    = (ac.get("callsign") or ac.get("flight") or "").strip() or None

        try: gs    = float(gs) if gs is not None else None
        except: gs = None
        try: track = float(track) if track is not None else None
        except: track = None
        try: vr    = int(vr) if vr is not None else None
        except: vr = None

        return RawADSBMessage(
            receiver_id   = "adsb-fi",
            recv_time     = now,
            icao24        = icao,
            raw_message   = "",
            msg_type      = 17,
            callsign      = cs,
            lat           = lat,
            lon           = lon,
            altitude_baro = alt_baro,
            altitude_geo  = alt_geo,
            velocity      = gs,
            heading       = track,
            vertical_rate = vr,
            on_ground     = (ac.get("alt_baro") == "ground" or ac.get("on_ground") is True),
            squawk        = str(ac["squawk"]) if ac.get("squawk") else None,
        )


# ─── Source 3: airplanes.live ─────────────────────────────────────────────────
# Community aggregator with excellent coverage especially in the Americas.
# Docs: https://airplanes.live/api-reference/

class AirplanesLiveSource:
    URL      = "https://api.airplanes.live/v2/point/0/0/90000"  # full global bbox hack
    URLS     = [
        # Tile the world in 4 quadrants to get all aircraft
        "https://api.airplanes.live/v2/point/45/0/20000",
        "https://api.airplanes.live/v2/point/-45/0/20000",
        "https://api.airplanes.live/v2/point/45/180/20000",
        "https://api.airplanes.live/v2/point/-45/180/20000",
    ]
    INTERVAL = 12

    def __init__(self) -> None:
        self.name = "airplanes.live"

    async def fetch(self, session: aiohttp.ClientSession) -> List[RawADSBMessage]:
        all_msgs: Dict[str, RawADSBMessage] = {}
        now = time.time()

        async def fetch_tile(url):
            try:
                async with session.get(
                    url, headers=HEADERS,
                    timeout=aiohttp.ClientTimeout(total=15)
                ) as resp:
                    if resp.status != 200:
                        return []
                    data = await resp.json(content_type=None)
                    aircraft = data.get("ac") or data.get("aircraft") or []
                    return aircraft
            except Exception as e:
                log.debug("[airplanes.live] tile error: %s", e)
                return []

        tasks = [fetch_tile(u) for u in self.URLS]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for res in results:
            if isinstance(res, list):
                for ac in res:
                    msg = self._parse(ac, now)
                    if msg and msg.icao24 not in all_msgs:
                        all_msgs[msg.icao24] = msg

        log.info("[airplanes.live] %d aircraft", len(all_msgs))
        return list(all_msgs.values())

    def _parse(self, ac: dict, now: float) -> Optional[RawADSBMessage]:
        icao = (ac.get("hex") or ac.get("icao") or "").upper().strip()
        if not icao or len(icao) != 6:
            return None

        lat = ac.get("lat")
        lon = ac.get("lon")
        if lat is None or lon is None:
            return None
        try:
            lat, lon = float(lat), float(lon)
        except: return None

        alt_baro = ac.get("alt_baro")
        alt_geo  = ac.get("alt_geom")
        try: alt_baro = int(alt_baro) if alt_baro not in (None, "ground", "grnd") else 0
        except: alt_baro = None
        try: alt_geo  = int(alt_geo) if alt_geo is not None else None
        except: alt_geo = None

        gs    = ac.get("gs")
        track = ac.get("track")
        vr    = ac.get("baro_rate")
        cs    = (ac.get("flight") or "").strip() or None

        try: gs    = float(gs) if gs is not None else None
        except: gs = None
        try: track = float(track) if track is not None else None
        except: track = None
        try: vr    = int(vr) if vr is not None else None
        except: vr = None

        return RawADSBMessage(
            receiver_id   = "airplanes-live",
            recv_time     = now,
            icao24        = icao,
            raw_message   = "",
            msg_type      = 17,
            callsign      = cs,
            lat           = lat,
            lon           = lon,
            altitude_baro = alt_baro,
            altitude_geo  = alt_geo,
            velocity      = gs,
            heading       = track,
            vertical_rate = vr,
            on_ground     = (ac.get("alt_baro") in ("ground", "grnd")),
            squawk        = str(ac["squawk"]) if ac.get("squawk") else None,
            nic           = ac.get("nic"),
        )


# ─── Source 4: OpenSky (fallback / supplemental) ──────────────────────────────

class OpenSkySource:
    URL      = "https://opensky-network.org/api/states/all"
    INTERVAL = 15

    def __init__(self, username: str = "", password: str = "") -> None:
        self.auth = aiohttp.BasicAuth(username, password) if username else None
        self.name = "opensky"

    async def fetch(self, session: aiohttp.ClientSession) -> List[RawADSBMessage]:
        try:
            async with session.get(
                self.URL,
                auth=self.auth,
                timeout=aiohttp.ClientTimeout(total=20)
            ) as resp:
                if resp.status == 429:
                    log.warning("[opensky] Rate limited — skipping")
                    return []
                if resp.status != 200:
                    log.warning("[opensky] HTTP %d", resp.status)
                    return []
                data = await resp.json(content_type=None)
                states = data.get("states") or []
                now = float(data.get("time", time.time()))
                results = []
                for s in states:
                    msg = self._parse(s, now)
                    if msg:
                        results.append(msg)
                log.info("[opensky] %d aircraft", len(results))
                return results
        except Exception as e:
            log.error("[opensky] fetch error: %s", e)
            return []

    def _parse(self, s: list, now: float) -> Optional[RawADSBMessage]:
        if not s or not s[0]:
            return None
        icao = s[0].upper().strip()
        if len(icao) != 6:
            return None

        lat = s[6]
        lon = s[5]
        if lat is None or lon is None:
            return None

        alt_baro = int(s[7]  * 3.28084) if s[7]  is not None else None
        alt_geo  = int(s[13] * 3.28084) if s[13] is not None else None
        vel      = int(s[9]  * 1.944)   if s[9]  is not None else None
        vr       = int(s[11] * 196.85)  if s[11] is not None else None
        cs       = (s[1] or "").strip() or None

        return RawADSBMessage(
            receiver_id   = "opensky",
            recv_time     = now,
            icao24        = icao,
            raw_message   = "",
            msg_type      = 17,
            callsign      = cs,
            lat           = float(lat),
            lon           = float(lon),
            altitude_baro = alt_baro,
            altitude_geo  = alt_geo,
            velocity      = vel,
            heading       = s[10],
            vertical_rate = vr,
            on_ground     = bool(s[8]) if s[8] is not None else False,
            squawk        = s[14],
        )


# ─── Multi-source merger ───────────────────────────────────────────────────────

class MultiSourceIngestor:
    """
    Polls all sources concurrently and merges results.
    Deduplication: last writer wins, but prefers sources with more complete data.
    """

    def __init__(self) -> None:
        self.sources = [
            ADSBLolSource(),
            ADSBFiSource(),
            AirplanesLiveSource(),
            OpenSkySource(
                username=settings.OPENSKY_USERNAME,
                password=settings.OPENSKY_PASSWORD,
            ),
        ]

    async def fetch_all(self, session: aiohttp.ClientSession) -> Dict[str, RawADSBMessage]:
        tasks = [src.fetch(session) for src in self.sources]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        merged: Dict[str, RawADSBMessage] = {}

        for msgs in results:
            if isinstance(msgs, Exception):
                log.error("Source error: %s", msgs)
                continue
            for msg in msgs:
                existing = merged.get(msg.icao24)
                if existing is None:
                    merged[msg.icao24] = msg
                else:
                    # Prefer the message with more fields populated
                    existing_score = sum([
                        existing.lat is not None,
                        existing.velocity is not None,
                        existing.altitude_baro is not None,
                        existing.callsign is not None,
                        existing.heading is not None,
                    ])
                    new_score = sum([
                        msg.lat is not None,
                        msg.velocity is not None,
                        msg.altitude_baro is not None,
                        msg.callsign is not None,
                        msg.heading is not None,
                    ])
                    if new_score >= existing_score:
                        # Merge: take best fields from both
                        merged[msg.icao24] = RawADSBMessage(
                            receiver_id   = msg.receiver_id,
                            recv_time     = msg.recv_time,
                            icao24        = msg.icao24,
                            raw_message   = "",
                            msg_type      = 17,
                            callsign      = msg.callsign or existing.callsign,
                            lat           = msg.lat or existing.lat,
                            lon           = msg.lon or existing.lon,
                            altitude_baro = msg.altitude_baro or existing.altitude_baro,
                            altitude_geo  = msg.altitude_geo  or existing.altitude_geo,
                            velocity      = msg.velocity      or existing.velocity,
                            heading       = msg.heading       or existing.heading,
                            vertical_rate = msg.vertical_rate or existing.vertical_rate,
                            on_ground     = msg.on_ground,
                            squawk        = msg.squawk        or existing.squawk,
                            nic           = msg.nic           or existing.nic,
                        )

        log.info("Merged total: %d unique aircraft from %d sources",
                 len(merged), len(self.sources))
        return merged


# ─── Main loop ────────────────────────────────────────────────────────────────

async def run() -> None:
    logging.basicConfig(
        level=settings.LOG_LEVEL,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    log.info("Starting multi-source ADS-B ingestor")

    producer = await make_producer()
    ingestor = MultiSourceIngestor()
    total_published = 0
    poll_interval = 8   # seconds between full refreshes

    connector = aiohttp.TCPConnector(limit=20, ssl=False)
    async with aiohttp.ClientSession(connector=connector) as session:
        while True:
            t0 = time.time()
            try:
                merged = await ingestor.fetch_all(session)

                for msg in merged.values():
                    await publish(producer, msg)
                    total_published += 1

                elapsed = time.time() - t0
                log.info(
                    "Published %d aircraft (total: %d) in %.1fs",
                    len(merged), total_published, elapsed
                )

            except Exception as e:
                log.error("Ingestor loop error: %s", e)

            # Wait remainder of interval
            sleep_for = max(1.0, poll_interval - (time.time() - t0))
            await asyncio.sleep(sleep_for)


if __name__ == "__main__":
    asyncio.run(run())
