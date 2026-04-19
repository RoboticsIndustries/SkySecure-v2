# SkySecure V2 — Global Airspace Intelligence Platform

Real-time, security-focused airspace awareness system combining ADS-B, MLAT,
and ACARS data into a unified threat-scored live map.

## Architecture

```
SDR Receivers / OpenSky API
         │
    [ADS-B Ingestor]
         │
    [Kafka: raw.adsb]
         │           ╲
    [MLAT Solver]   [Fusion Engine] ──→ [Kafka: fused.tracks]
         │           /                          │
    [Kafka: raw.mlat]               [Anomaly Detector]
                                               │
                                    [Military Classifier]
                                               │
                                    [FastAPI + WebSocket]
                                               │
                                    [React Radar Frontend]
```

## Quick Start

### Prerequisites
- Docker + Docker Compose
- (Optional) RTL-SDR dongle + dump1090 for local reception

### Run with OpenSky (no hardware needed)
```bash
git clone <repo>
cd skysecure-v2
cp .env.example .env
docker-compose up -d
```

Open http://localhost:3000

### Run with local SDR
```bash
# Install dump1090
sudo apt install dump1090-mutability

# Start dump1090
dump1090 --net --net-beast

# Update .env
ADSB_SOURCE=beast
DUMP1090_HOST=localhost

docker-compose up -d
```

## Services

| Service | Port | Description |
|---------|------|-------------|
| Frontend | 3000 | React radar map |
| API | 8000 | REST + WebSocket |
| Kafka | 9092 | Message bus |
| Redis | 6379 | State store |
| Postgres | 5432 | Persistent store |

## API

```
GET  /api/aircraft              All live tracks
GET  /api/aircraft/{icao}       Single aircraft detail
GET  /api/alerts                Active anomaly alerts
GET  /api/stats                 System statistics
WS   /ws/tracks                 Real-time broadcast
```

## Detection Capabilities

| Capability | Method |
|-----------|--------|
| Civil aircraft | ADS-B decode |
| GNSS spoofing | Baro/Geo altitude delta, position conflict |
| Identity spoofing | Duplicate ICAO detection |
| Ghost aircraft | ADS-B without MLAT confirmation |
| Silent aircraft | MLAT-only tracks |
| Military aircraft | ICAO block + behavioral scoring |
| Formation flying | Multi-track proximity analysis |
| Transponder off | Signal loss detection |
| Anomalous trajectory | LSTM prediction error |

## Project Structure

```
skysecure-v2/
├── models.py              Core data models (StateVector, etc.)
├── config.py              All configuration
├── docker-compose.yml     Full stack orchestration
├── ingestion/
│   └── adsb_receiver.py   OpenSky / SBS / Beast sources
├── processing/
│   ├── mlat_solver.py     TDOA-based position solver
│   └── fusion_engine.py   Multi-source data fusion + Kalman
├── anomaly/
│   └── detector.py        3-layer anomaly detection
├── military/
│   └── classifier.py      P(military) scoring
├── api/
│   └── main.py            FastAPI REST + WebSocket
└── frontend/
    └── src/
        ├── App.jsx         Radar map UI
        ├── hooks/          WebSocket client
        └── store/          Zustand state
```
# SkySecure-v2
