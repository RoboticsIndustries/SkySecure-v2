"""
api/main.py
────────────
FastAPI application.

Key addition: /api/live-aircraft
  Fetches directly from adsb.lol (ADS-B Exchange community feed) on the
  server side, bypassing browser CORS restrictions entirely.
  Caches for 10 seconds. Returns all globally tracked aircraft — typically
  10,000–18,000 at any given time.
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Optional, List, Dict, Any

import aiohttp
import orjson
import redis.asyncio as aioredis
from aiokafka import AIOKafkaConsumer
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import ORJSONResponse
from contextlib import asynccontextmanager

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from models import StateVector, RiskBand, Classification
from config import settings

log = logging.getLogger(__name__)

# ─── Global state ──────────────────────────────────────────────────────────────

redis_client: Optional[aioredis.Redis] = None
_ws_clients: set[WebSocket] = set()
_track_snapshot: List[Dict[str, Any]] = []

# Live aircraft cache — avoids hammering adsb.lol on every request
_live_cache: Dict[str, Any] = {
    "ts":       0,
    "aircraft": [],
}
LIVE_CACHE_TTL = 30   # seconds — single OpenSky request every 30s

# Sources to try in order (server-side, no CORS issues)
# OpenSky single global endpoint — most reliable approach

HEADERS = {
    "User-Agent": "SkySecure/2.0 (airspace research)",
    "Accept":     "application/json",
}


# ─── Lifespan ─────────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    global redis_client
    redis_client = aioredis.from_url(settings.REDIS_URL, decode_responses=False)
    asyncio.create_task(broadcast_loop())
    asyncio.create_task(alert_consumer_loop())
    yield
    await redis_client.close()


# ─── App ──────────────────────────────────────────────────────────────────────

app = FastAPI(
    title="SkySecure V2 API",
    version="2.0.0",
    default_response_class=ORJSONResponse,
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],   # open — frontend is a local file
    allow_methods=["*"],
    allow_headers=["*"],
)





# ─── UNUSED (kept for compatibility) ─────────────────────────────────────────

def _normalise(raw: dict, src: str) -> Optional[dict]:
    icao = (raw.get("hex") or raw.get("icao") or raw.get("icao24") or "").upper().strip()
    if not icao or len(icao) > 8:
        return None

    lat = raw.get("lat")
    lon = raw.get("lon") or raw.get("lng")
    if lat is None or lon is None:
        return None

    try:
        lat, lon = float(lat), float(lon)
    except (TypeError, ValueError):
        return None

    if not (-90 <= lat <= 90 and -180 <= lon <= 180):
        return None

    alt_raw = raw.get("alt_baro") or raw.get("altitude") or raw.get("baro_altitude")
    try:
        alt = int(alt_raw) if alt_raw not in (None, "ground", "grnd") else 0
    except (TypeError, ValueError):
        alt = None

    vel_raw = raw.get("gs") or raw.get("velocity") or raw.get("speed")
    try:
        vel = round(float(vel_raw)) if vel_raw is not None else None
    except (TypeError, ValueError):
        vel = None

    hdg_raw = raw.get("track") or raw.get("heading") or raw.get("true_track")
    try:
        hdg = float(hdg_raw) if hdg_raw is not None else None
    except (TypeError, ValueError):
        hdg = None

    vr_raw = raw.get("baro_rate") or raw.get("vertical_rate")
    try:
        vr = int(vr_raw) if vr_raw is not None else None
    except (TypeError, ValueError):
        vr = None

    cs = (raw.get("flight") or raw.get("callsign") or "").strip() or None

    return {
        "icao":  icao,
        "cs":    cs,
        "lat":   lat,
        "lon":   lon,
        "alt":   alt,
        "vel":   vel,
        "hdg":   hdg,
        "vr":    vr,
        "gnd":   alt_raw in ("ground", "grnd") or raw.get("on_ground") is True,
        "src":   src,
        "risk":  0,
        "anoms": [],
        "cls":   "CIVILIAN",
        "conf":  0.85,
        "mil":   0.0,
        "band":  "NORMAL",
        "trail": [],
    }


# ─── Live aircraft — single OpenSky global request ───────────────────────────

async def _fetch_live_aircraft() -> List[dict]:
    """
    Single OpenSky global request — simple and reliable.
    Returns all aircraft currently tracked. Cached 30s to respect rate limits.
    """
    try:
        connector = aiohttp.TCPConnector(ssl=True)
        async with aiohttp.ClientSession(connector=connector, headers=HEADERS) as session:
            async with session.get(
                "https://opensky-network.org/api/states/all",
                timeout=aiohttp.ClientTimeout(total=25),
            ) as resp:
                if resp.status != 200:
                    log.warning("OpenSky returned HTTP %d", resp.status)
                    return []
                data = await resp.json(content_type=None)
                states = data.get("states") or []

        aircraft = []
        for s in states:
            if not s or s[0] is None or s[5] is None or s[6] is None:
                continue
            try:
                lat, lon = float(s[6]), float(s[5])
            except (TypeError, ValueError):
                continue
            if not (-90 <= lat <= 90 and -180 <= lon <= 180):
                continue
            icao = s[0].upper().strip()
            if len(icao) != 6:
                continue

            def ft(m):
                try: return int(float(m) * 3.28084) if m else None
                except: return None
            def kts(ms):
                try: return int(float(ms) * 1.944) if ms else None
                except: return None

            aircraft.append({
                "icao": icao,
                "cs":   (s[1] or "").strip() or None,
                "lat":  lat, "lon": lon,
                "alt":  ft(s[7]), "vel": kts(s[9]),
                "hdg":  float(s[10]) if s[10] else None,
                "vr":   int(float(s[11]) * 196.85) if s[11] else None,
                "gnd":  bool(s[8]), "src": "opensky",
                "risk": 0, "anoms": [], "cls": "CIVILIAN",
                "conf": 0.85, "mil": 0.0, "band": "NORMAL", "trail": [],
            })

        log.info("OpenSky: %d aircraft", len(aircraft))
        return aircraft

    except Exception as e:
        log.error("OpenSky fetch failed: %s", e)
        return []


# ─── REST Endpoints ───────────────────────────────────────────────────────────

@app.get("/api/live-aircraft")
async def get_live_aircraft():
    """
    Server-side proxy for ADS-B Exchange / adsb.lol.
    Returns all globally tracked aircraft — bypasses browser CORS.
    Cached for 10 seconds.
    """
    global _live_cache

    now = time.time()
    if now - _live_cache["ts"] < LIVE_CACHE_TTL and _live_cache["aircraft"]:
        return {
            "count":    len(_live_cache["aircraft"]),
            "source":   "cache",
            "aircraft": _live_cache["aircraft"],
        }

    aircraft = await _fetch_live_aircraft()

    if aircraft:
        _live_cache = {"ts": now, "aircraft": aircraft}

    return {
        "count":    len(aircraft),
        "source":   "live",
        "aircraft": aircraft,
    }


@app.get("/api/aircraft")
async def get_all_aircraft(
    limit: int = Query(5000, le=20000),
    min_risk: int = Query(0, ge=0, le=100),
):
    """
    Return state vectors from the Redis fusion pipeline.
    Combined with /api/live-aircraft on the frontend for full coverage.
    """
    keys = await redis_client.keys("sv:*")
    results = []

    if keys:
        pipe = redis_client.pipeline()
        for k in keys:
            pipe.get(k)
        raw_values = await pipe.execute()

        for raw in raw_values:
            if not raw:
                continue
            try:
                sv = StateVector.from_bytes(raw)
                if sv.risk_score >= min_risk:
                    results.append(sv.to_api_dict())
            except Exception:
                continue

    return {"count": len(results), "timestamp": time.time(), "aircraft": results[:limit]}


@app.get("/api/alerts")
async def get_alerts(limit: int = Query(100, le=1000), min_score: int = Query(50)):
    keys = await redis_client.keys("sv:*")
    alerts = []
    if keys:
        pipe = redis_client.pipeline()
        for k in keys:
            pipe.get(k)
        for raw in await pipe.execute():
            if not raw:
                continue
            try:
                sv = StateVector.from_bytes(raw)
                if sv.risk_score >= min_score and sv.anomalies:
                    alerts.append({
                        "icao24":         sv.icao24,
                        "callsign":       sv.callsign,
                        "risk_score":     sv.risk_score,
                        "risk_band":      sv.risk_band.value,
                        "classification": sv.classification.value,
                        "anomalies": [{"type": a.anomaly_type.value, "description": a.description} for a in sv.anomalies],
                        "lat":            sv.lat,
                        "lon":            sv.lon,
                        "last_seen":      sv.last_seen,
                    })
            except Exception:
                continue
    alerts.sort(key=lambda x: x["risk_score"], reverse=True)
    return {"count": len(alerts), "alerts": alerts[:limit]}


@app.get("/api/stats")
async def get_stats():
    keys = await redis_client.keys("sv:*")
    total = len(keys) if keys else 0
    classifications = {c.value: 0 for c in Classification}
    risk_bands = {b.value: 0 for b in RiskBand}
    if keys:
        pipe = redis_client.pipeline()
        for k in keys:
            pipe.get(k)
        for raw in await pipe.execute():
            if not raw:
                continue
            try:
                sv = StateVector.from_bytes(raw)
                classifications[sv.classification.value] += 1
                risk_bands[sv.risk_band.value] += 1
            except Exception:
                continue
    return {
        "timestamp":       time.time(),
        "total_tracks":    total,
        "classifications": classifications,
        "risk_bands":      risk_bands,
        "ws_clients":      len(_ws_clients),
    }


@app.get("/healthz")
async def healthz():
    return {"status": "ok", "time": time.time()}


# ─── WebSocket ────────────────────────────────────────────────────────────────

@app.websocket("/ws/tracks")
async def ws_tracks(websocket: WebSocket):
    await websocket.accept()
    _ws_clients.add(websocket)
    try:
        # Send initial snapshot
        payload = orjson.dumps({
            "type":     "snapshot",
            "ts":       time.time(),
            "count":    len(_track_snapshot),
            "aircraft": _track_snapshot,
        })
        await websocket.send_bytes(payload)

        while True:
            try:
                data = await asyncio.wait_for(websocket.receive_text(), timeout=30.0)
                if data == "ping":
                    await websocket.send_text("pong")
            except asyncio.TimeoutError:
                await websocket.send_text("ping")
    except WebSocketDisconnect:
        pass
    except Exception:
        pass
    finally:
        _ws_clients.discard(websocket)


# ─── Background: broadcast loop ───────────────────────────────────────────────

async def broadcast_loop() -> None:
    """Broadcast all aircraft from Redis to WebSocket clients every second."""
    global _track_snapshot

    while True:
        await asyncio.sleep(settings.WS_BROADCAST_INTERVAL)
        try:
            keys = await redis_client.keys("ac:*")  # direct aircraft store
            fused_keys = await redis_client.keys("sv:*")  # fusion pipeline

            all_keys = list(set(keys + fused_keys))
            tracks = []

            if all_keys:
                pipe = redis_client.pipeline()
                for k in all_keys:
                    pipe.get(k)
                for raw in await pipe.execute():
                    if not raw:
                        continue
                    try:
                        import orjson as _oj
                        ac = _oj.loads(raw)
                        if isinstance(ac, dict) and ac.get("icao"):
                            tracks.append(ac)
                            continue
                    except Exception:
                        pass
                    try:
                        sv = StateVector.from_bytes(raw)
                        tracks.append(sv.to_api_dict())
                    except Exception:
                        continue

            _track_snapshot = tracks

            if not _ws_clients:
                continue

            payload = orjson.dumps({
                "type":     "snapshot",
                "ts":       time.time(),
                "count":    len(tracks),
                "aircraft": tracks,
            })

            dead = set()
            for ws in _ws_clients:
                try:
                    await ws.send_bytes(payload)
                except Exception:
                    dead.add(ws)
            for ws in dead:
                _ws_clients.discard(ws)

        except Exception as e:
            log.error("Broadcast loop error: %s", e)


# ─── Background: alert consumer ───────────────────────────────────────────────

async def alert_consumer_loop() -> None:
    consumer = AIOKafkaConsumer(
        settings.TOPIC_ALERTS_ANOMALY,
        bootstrap_servers=settings.KAFKA_BOOTSTRAP,
        group_id=f"{settings.KAFKA_GROUP_PREFIX}.api-alerts",
        value_deserializer=lambda v: v,
        auto_offset_reset="latest",
    )
    await consumer.start()
    try:
        async for msg in consumer:
            if not _ws_clients:
                continue
            try:
                sv = StateVector.from_bytes(msg.value)
                alert_payload = orjson.dumps({
                    "type":     "alert",
                    "ts":       time.time(),
                    "aircraft": sv.to_api_dict(),
                    "anomalies": [{"type": a.anomaly_type.value, "description": a.description} for a in sv.anomalies],
                })
                dead = set()
                for ws in _ws_clients:
                    try:
                        await ws.send_bytes(alert_payload)
                    except Exception:
                        dead.add(ws)
                for ws in dead:
                    _ws_clients.discard(ws)
            except Exception as e:
                log.error("Alert push error: %s", e)
    finally:
        await consumer.stop()