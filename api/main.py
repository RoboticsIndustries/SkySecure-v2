"""
api/main.py
────────────
FastAPI application with TDOA validation integration.

Key additions:
- TDOA validator for physics-based spoofing detection
- Enhanced anomaly detector with 4-layer scoring
- New endpoints: /api/tdoa/receivers, /api/tdoa/validate
- TDOA status in WebSocket broadcasts
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

# TDOA imports
try:
    from processing.tdoa_validator import TDOAValidator, create_test_receivers, Position
    from anomaly.enhanced_detector import EnhancedAnomalyDetector
    TDOA_AVAILABLE = True
except ImportError:
    TDOA_AVAILABLE = False
    logging.warning("TDOA modules not available - running without TDOA validation")

log = logging.getLogger(__name__)

# ─── Global state ──────────────────────────────────────────────────────────────

redis_client: Optional[aioredis.Redis] = None
_ws_clients: set[WebSocket] = set()
_track_snapshot: List[Dict[str, Any]] = []

# TDOA validator instances
tdoa_validator: Optional[TDOAValidator] = None
anomaly_detector: Optional[EnhancedAnomalyDetector] = None

# Live aircraft cache
_live_cache: Dict[str, Any] = {
    "ts":       0,
    "aircraft": [],
}
LIVE_CACHE_TTL = 30   # seconds

HEADERS = {
    "User-Agent": "SkySecure/2.0 (airspace research)",
    "Accept":     "application/json",
}


# ─── TDOA Helper Functions ────────────────────────────────────────────────────

def simulate_receiver_timestamps(lat: float, lon: float, alt: float) -> Dict[str, float]:
    """
    Simulate what receiver timestamps would be for an aircraft position.
    In production, these would come from actual receiver hardware.
    """
    if not tdoa_validator:
        return {}
    
    try:
        aircraft_pos = Position.from_lat_lon_alt(lat, lon, alt)
        t0 = time.time()
        
        receive_times = {}
        for rid, receiver in tdoa_validator.receivers.items():
            distance = aircraft_pos.distance_to(receiver.position)
            propagation_time = distance / 299792458  # speed of light
            receive_time = t0 + propagation_time + receiver.clock_offset * 1e-9
            receive_times[rid] = receive_time
        
        return receive_times
    except Exception as e:
        log.error(f"Error simulating timestamps: {e}")
        return {}


def validate_aircraft_tdoa(aircraft: dict) -> dict:
    """
    Run TDOA validation on an aircraft and add results to the dict.
    """
    if not tdoa_validator or not anomaly_detector:
        return aircraft
    
    try:
        lat = aircraft.get('lat')
        lon = aircraft.get('lon')
        alt = aircraft.get('alt', 0)
        
        if lat is None or lon is None:
            return aircraft
        
        # Simulate receiver timestamps (in production, get from actual receivers)
        receive_times = simulate_receiver_timestamps(lat, lon, alt)
        
        if not receive_times or len(receive_times) < 4:
            return aircraft
        
        # Run TDOA validation
        tdoa_result = tdoa_validator.validate_position(
            icao=aircraft.get('icao', 'UNKNOWN'),
            claimed_lat=lat,
            claimed_lon=lon,
            claimed_alt=alt,
            receive_times=receive_times
        )
        
        # Add TDOA results to aircraft dict
        aircraft['tdoa'] = {
            'validated': tdoa_result.is_valid,
            'verdict': tdoa_result.verdict,
            'error_m': round(tdoa_result.max_error_meters, 1),
            'confidence': round(tdoa_result.confidence, 3)
        }
        
        # Run full anomaly detection if we have velocity data
        if aircraft.get('vel') is not None:
            anomaly_score = anomaly_detector.calculate_overall_score(
                icao=aircraft.get('icao', 'UNKNOWN'),
                lat=lat,
                lon=lon,
                alt_baro=alt,
                alt_geo=alt,  # Assume same for now
                velocity=aircraft.get('vel', 0),
                vertical_rate=aircraft.get('vr', 0),
                heading=aircraft.get('hdg', 0),
                receive_times=receive_times
            )
            
            # Update risk scoring based on TDOA + anomaly detection
            aircraft['threat_level'] = anomaly_score.threat_level
            aircraft['anomaly_score'] = round(anomaly_score.overall_score, 2)
            aircraft['tdoa_spoofing_score'] = round(anomaly_score.tdoa_spoofing, 2)
            
            # Override risk if TDOA detects spoofing
            if tdoa_result.verdict == "SPOOFED":
                aircraft['risk'] = max(aircraft.get('risk', 0), 80)
                aircraft['band'] = 'HIGH'
                if 'anoms' not in aircraft:
                    aircraft['anoms'] = []
                aircraft['anoms'].append({
                    'type': 'TDOA_SPOOFING',
                    'description': f'Position spoofing detected: {tdoa_result.max_error_meters:.0f}m error'
                })
        
    except Exception as e:
        log.error(f"TDOA validation error for {aircraft.get('icao')}: {e}")
    
    return aircraft


# ─── Lifespan ─────────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    global redis_client, tdoa_validator, anomaly_detector
    
    redis_client = aioredis.from_url(settings.REDIS_URL, decode_responses=False)
    
    # Initialize TDOA validator
    if TDOA_AVAILABLE:
        try:
            receivers = create_test_receivers()
            tdoa_validator = TDOAValidator(receivers)
            anomaly_detector = EnhancedAnomalyDetector(tdoa_validator)
            log.info(f"✅ TDOA validator initialized with {len(receivers)} receivers")
        except Exception as e:
            log.error(f"Failed to initialize TDOA validator: {e}")
            tdoa_validator = None
            anomaly_detector = None
    
    asyncio.create_task(broadcast_loop())
    asyncio.create_task(alert_consumer_loop())
    
    yield
    
    await redis_client.close()


# ─── App ──────────────────────────────────────────────────────────────────────

app = FastAPI(
    title="SkySecure V2 API with TDOA",
    version="2.1.0",
    default_response_class=ORJSONResponse,
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ─── Live aircraft fetching ───────────────────────────────────────────────────

async def _fetch_live_aircraft() -> List[dict]:
    """
    Fetch from OpenSky and apply TDOA validation to each aircraft.
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

            ac = {
                "icao": icao,
                "cs":   (s[1] or "").strip() or None,
                "lat":  lat, "lon": lon,
                "alt":  ft(s[7]), "vel": kts(s[9]),
                "hdg":  float(s[10]) if s[10] else None,
                "vr":   int(float(s[11]) * 196.85) if s[11] else None,
                "gnd":  bool(s[8]), "src": "opensky",
                "risk": 0, "anoms": [], "cls": "CIVILIAN",
                "conf": 0.85, "mil": 0.0, "band": "NORMAL", "trail": [],
            }
            
            # Apply TDOA validation
            if TDOA_AVAILABLE and tdoa_validator:
                ac = validate_aircraft_tdoa(ac)
            
            aircraft.append(ac)

        log.info(f"OpenSky: {len(aircraft)} aircraft (TDOA: {'enabled' if TDOA_AVAILABLE else 'disabled'})")
        return aircraft

    except Exception as e:
        log.error("OpenSky fetch failed: %s", e)
        return []


# ─── REST Endpoints ───────────────────────────────────────────────────────────

@app.get("/api/live-aircraft")
async def get_live_aircraft():
    """
    Server-side proxy for OpenSky with TDOA validation.
    Returns all globally tracked aircraft with spoofing detection.
    Cached for 30 seconds.
    """
    global _live_cache

    now = time.time()
    if now - _live_cache["ts"] < LIVE_CACHE_TTL and _live_cache["aircraft"]:
        return {
            "count":    len(_live_cache["aircraft"]),
            "source":   "cache",
            "aircraft": _live_cache["aircraft"],
            "tdoa_enabled": TDOA_AVAILABLE,
        }

    aircraft = await _fetch_live_aircraft()

    if aircraft:
        _live_cache = {"ts": now, "aircraft": aircraft}

    return {
        "count":    len(aircraft),
        "source":   "live",
        "aircraft": aircraft,
        "tdoa_enabled": TDOA_AVAILABLE,
    }


@app.get("/api/aircraft")
async def get_all_aircraft(
    limit: int = Query(5000, le=20000),
    min_risk: int = Query(0, ge=0, le=100),
):
    """
    Return state vectors from the Redis fusion pipeline with TDOA validation.
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
                    ac = sv.to_api_dict()
                    
                    # Apply TDOA validation
                    if TDOA_AVAILABLE and tdoa_validator:
                        ac = validate_aircraft_tdoa(ac)
                    
                    results.append(ac)
            except Exception:
                continue

    return {
        "count": len(results),
        "timestamp": time.time(),
        "aircraft": results[:limit],
        "tdoa_enabled": TDOA_AVAILABLE,
    }


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
                    alert = {
                        "icao24":         sv.icao24,
                        "callsign":       sv.callsign,
                        "risk_score":     sv.risk_score,
                        "risk_band":      sv.risk_band.value,
                        "classification": sv.classification.value,
                        "anomalies": [{"type": a.anomaly_type.value, "description": a.description} for a in sv.anomalies],
                        "lat":            sv.lat,
                        "lon":            sv.lon,
                        "last_seen":      sv.last_seen,
                    }
                    
                    # Add TDOA validation status if available
                    if TDOA_AVAILABLE and tdoa_validator and sv.lat and sv.lon:
                        ac = {"icao": sv.icao24, "lat": sv.lat, "lon": sv.lon, "alt": getattr(sv, 'altitude', 0)}
                        ac = validate_aircraft_tdoa(ac)
                        if 'tdoa' in ac:
                            alert['tdoa'] = ac['tdoa']
                    
                    alerts.append(alert)
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
    
    tdoa_stats = {"validated": 0, "spoofed": 0, "uncertain": 0}
    
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
        "tdoa_enabled":    TDOA_AVAILABLE,
        "tdoa_stats":      tdoa_stats,
    }


# ─── TDOA-specific endpoints ──────────────────────────────────────────────────

@app.get("/api/tdoa/receivers")
async def get_tdoa_receivers():
    """Get TDOA receiver network status"""
    if not TDOA_AVAILABLE or not tdoa_validator:
        return {"error": "TDOA not available", "receivers": []}
    
    try:
        return {
            "count": len(tdoa_validator.receivers),
            "receivers": tdoa_validator.get_receiver_status()
        }
    except Exception as e:
        return {"error": str(e), "receivers": []}


@app.post("/api/tdoa/validate")
async def validate_position_tdoa(
    icao: str,
    lat: float,
    lon: float,
    alt: float = 0,
):
    """Manually validate an aircraft position with TDOA"""
    if not TDOA_AVAILABLE or not tdoa_validator:
        return {"error": "TDOA not available"}
    
    try:
        # Simulate receiver timestamps
        receive_times = simulate_receiver_timestamps(lat, lon, alt)
        
        if not receive_times:
            return {"error": "Failed to simulate receiver timestamps"}
        
        result = tdoa_validator.validate_position(icao, lat, lon, alt, receive_times)
        
        return {
            "icao": icao,
            "position": {"lat": lat, "lon": lon, "alt": alt},
            "tdoa_result": result.to_dict()
        }
    except Exception as e:
        return {"error": str(e)}


@app.get("/healthz")
async def healthz():
    return {
        "status": "ok",
        "time": time.time(),
        "tdoa_enabled": TDOA_AVAILABLE,
        "tdoa_receivers": len(tdoa_validator.receivers) if tdoa_validator else 0,
    }


# ─── WebSocket ────────────────────────────────────────────────────────────────

@app.websocket("/ws/tracks")
async def ws_tracks(websocket: WebSocket):
    await websocket.accept()
    _ws_clients.add(websocket)
    try:
        # Send initial snapshot with TDOA data
        payload = orjson.dumps({
            "type":     "snapshot",
            "ts":       time.time(),
            "count":    len(_track_snapshot),
            "aircraft": _track_snapshot,
            "tdoa_enabled": TDOA_AVAILABLE,
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
    """Broadcast all aircraft with TDOA validation to WebSocket clients."""
    global _track_snapshot

    while True:
        await asyncio.sleep(settings.WS_BROADCAST_INTERVAL)
        try:
            keys = await redis_client.keys("ac:*")
            fused_keys = await redis_client.keys("sv:*")

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
                            # Apply TDOA validation
                            if TDOA_AVAILABLE and tdoa_validator:
                                ac = validate_aircraft_tdoa(ac)
                            tracks.append(ac)
                            continue
                    except Exception:
                        pass
                    try:
                        sv = StateVector.from_bytes(raw)
                        ac = sv.to_api_dict()
                        # Apply TDOA validation
                        if TDOA_AVAILABLE and tdoa_validator:
                            ac = validate_aircraft_tdoa(ac)
                        tracks.append(ac)
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
                "tdoa_enabled": TDOA_AVAILABLE,
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
                ac = sv.to_api_dict()
                
                # Apply TDOA validation to alerts
                if TDOA_AVAILABLE and tdoa_validator:
                    ac = validate_aircraft_tdoa(ac)
                
                alert_payload = orjson.dumps({
                    "type":     "alert",
                    "ts":       time.time(),
                    "aircraft": ac,
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
