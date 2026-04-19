"""
api/main.py
────────────
FastAPI application providing:

  GET  /api/aircraft              — all live tracks (paginated)
  GET  /api/aircraft/{icao}       — single aircraft detail
  GET  /api/alerts                — recent anomaly alerts
  GET  /api/stats                 — system statistics
  WS   /ws/tracks                 — real-time WebSocket broadcast

The WebSocket broadcasts a compressed diff of all active state vectors
every WS_BROADCAST_INTERVAL seconds.
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Optional, List, Dict, Any

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
from military.classifier import MilitaryClassifier

log = logging.getLogger(__name__)


# ─── Global State ─────────────────────────────────────────────────────────────

redis_client: Optional[aioredis.Redis] = None
military_classifier = MilitaryClassifier()

# Active WebSocket connections
_ws_clients: set[WebSocket] = set()

# Track snapshot cache (updated every broadcast interval)
_track_snapshot: List[Dict[str, Any]] = []
_stats: Dict[str, Any] = {
    "total_tracks": 0,
    "civilian": 0,
    "military": 0,
    "unknown": 0,
    "alerts": 0,
    "uptime_sec": time.time(),
}


# ─── Lifespan ─────────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    global redis_client
    redis_client = aioredis.from_url(settings.REDIS_URL, decode_responses=False)

    # Start background tasks
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
    allow_origins=settings.API_CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ─── REST Endpoints ───────────────────────────────────────────────────────────

@app.get("/api/aircraft")
async def get_all_aircraft(
    limit:          int = Query(500, le=5000),
    classification: Optional[str] = None,
    min_risk:       int = Query(0, ge=0, le=100),
    on_ground:      Optional[bool] = None,
):
    """
    Return all currently tracked aircraft.
    Optionally filter by classification, minimum risk score, or on_ground status.
    """
    keys = await redis_client.keys("sv:*")
    results = []

    # Batch fetch
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

                # Apply military classification
                sv = military_classifier.apply_classification(sv)

                # Filters
                if classification and sv.classification.value != classification.upper():
                    continue
                if sv.risk_score < min_risk:
                    continue
                if on_ground is not None and sv.on_ground != on_ground:
                    continue

                results.append(sv.to_api_dict())
            except Exception:
                continue

    return {
        "count": len(results),
        "timestamp": time.time(),
        "aircraft": results[:limit],
    }


@app.get("/api/aircraft/{icao24}")
async def get_aircraft(icao24: str):
    """Get detailed state vector for a specific aircraft."""
    key = f"sv:{icao24.upper()}"
    raw = await redis_client.get(key)

    if not raw:
        return ORJSONResponse({"error": "Aircraft not found"}, status_code=404)

    sv = StateVector.from_bytes(raw)
    sv = military_classifier.apply_classification(sv)

    # Return full detail (not just api_dict compact form)
    data = sv.model_dump()
    data["military_signals"] = {}   # Would include signals breakdown in production

    return data


@app.get("/api/alerts")
async def get_alerts(
    limit: int = Query(100, le=1000),
    min_score: int = Query(50, ge=0, le=100),
):
    """Return recent high-risk anomaly events."""
    keys = await redis_client.keys("sv:*")
    alerts = []

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
                if sv.risk_score >= min_score and sv.anomalies:
                    alerts.append({
                        "icao24": sv.icao24,
                        "callsign": sv.callsign,
                        "risk_score": sv.risk_score,
                        "risk_band": sv.risk_band.value,
                        "classification": sv.classification.value,
                        "anomalies": [
                            {
                                "type": a.anomaly_type.value,
                                "description": a.description,
                                "delta": a.score_delta,
                                "time": a.timestamp,
                            }
                            for a in sv.anomalies
                        ],
                        "lat": sv.lat,
                        "lon": sv.lon,
                        "altitude": sv.altitude_baro,
                        "last_seen": sv.last_seen,
                    })
            except Exception:
                continue

    alerts.sort(key=lambda x: x["risk_score"], reverse=True)
    return {"count": len(alerts), "alerts": alerts[:limit]}


@app.get("/api/stats")
async def get_stats():
    """System statistics."""
    keys = await redis_client.keys("sv:*")
    total = len(keys) if keys else 0

    classifications = {c.value: 0 for c in Classification}
    risk_bands = {b.value: 0 for b in RiskBand}

    if keys:
        pipe = redis_client.pipeline()
        for k in keys:
            pipe.get(k)
        raws = await pipe.execute()

        for raw in raws:
            if not raw:
                continue
            try:
                sv = StateVector.from_bytes(raw)
                classifications[sv.classification.value] += 1
                risk_bands[sv.risk_band.value] += 1
            except Exception:
                continue

    return {
        "timestamp": time.time(),
        "uptime_sec": time.time() - _stats["uptime_sec"],
        "total_tracks": total,
        "classifications": classifications,
        "risk_bands": risk_bands,
        "ws_clients": len(_ws_clients),
    }


@app.get("/healthz")
async def healthz():
    return {"status": "ok", "time": time.time()}


# ─── WebSocket ────────────────────────────────────────────────────────────────

@app.websocket("/ws/tracks")
async def ws_tracks(websocket: WebSocket):
    """
    Real-time aircraft track broadcast.
    Client receives JSON updates every WS_BROADCAST_INTERVAL seconds.

    Message format:
    {
      "type": "snapshot",
      "ts": <unix_time>,
      "count": N,
      "aircraft": [<api_dict>, ...]
    }
    """
    await websocket.accept()
    _ws_clients.add(websocket)
    log.info("WebSocket client connected. Total: %d", len(_ws_clients))

    try:
        # Send initial snapshot immediately
        payload = orjson.dumps({
            "type": "snapshot",
            "ts": time.time(),
            "count": len(_track_snapshot),
            "aircraft": _track_snapshot,
        })
        await websocket.send_bytes(payload)

        # Keep connection alive, receive pings
        while True:
            try:
                data = await asyncio.wait_for(websocket.receive_text(), timeout=30.0)
                if data == "ping":
                    await websocket.send_text("pong")
            except asyncio.TimeoutError:
                await websocket.send_text("ping")

    except WebSocketDisconnect:
        pass
    except Exception as e:
        log.debug("WebSocket error: %s", e)
    finally:
        _ws_clients.discard(websocket)
        log.info("WebSocket client disconnected. Total: %d", len(_ws_clients))


# ─── Background: Track Broadcast Loop ─────────────────────────────────────────

async def broadcast_loop() -> None:
    """
    Every WS_BROADCAST_INTERVAL seconds:
      1. Fetch all active SVs from Redis
      2. Build compact snapshot
      3. Broadcast to all connected WebSocket clients
    """
    global _track_snapshot

    while True:
        await asyncio.sleep(settings.WS_BROADCAST_INTERVAL)

        try:
            keys = await redis_client.keys("sv:*")
            if not keys:
                continue

            pipe = redis_client.pipeline()
            for k in keys:
                pipe.get(k)
            raws = await pipe.execute()

            tracks = []
            for raw in raws:
                if not raw:
                    continue
                try:
                    sv = StateVector.from_bytes(raw)
                    sv = military_classifier.apply_classification(sv)
                    tracks.append(sv.to_api_dict())
                except Exception:
                    continue

            _track_snapshot = tracks

            if not _ws_clients:
                continue

            payload = orjson.dumps({
                "type": "snapshot",
                "ts": time.time(),
                "count": len(tracks),
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


# ─── Background: Alert Consumer ───────────────────────────────────────────────

async def alert_consumer_loop() -> None:
    """
    Consume high-priority alerts from Kafka and push to WebSocket clients
    immediately (without waiting for next broadcast interval).
    """
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
                sv = military_classifier.apply_classification(sv)

                alert_payload = orjson.dumps({
                    "type": "alert",
                    "ts": time.time(),
                    "aircraft": sv.to_api_dict(),
                    "anomalies": [
                        {
                            "type": a.anomaly_type.value,
                            "description": a.description,
                            "delta": a.score_delta,
                        }
                        for a in sv.anomalies
                    ],
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
