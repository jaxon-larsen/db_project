# MusicBrainz Data Pipeline

This project builds a repeatable instrument-trend dataset from MusicBrainz. It discovers instruments, harvests recording metadata, stages data in PostgreSQL, and serves analytics-ready rows from ClickHouse.

## End-to-end flow

`MusicBrainz API -> PostgreSQL (staging) -> ClickHouse (analytics)`

Airflow orchestrates this flow with the DAG `musicbrainz_global_pipeline`.

## What each project part does

| Path | Responsibility |
|---|---|
| `dags/main_dag.py` | Defines pipeline task order, retries, and dependencies (`scout_instruments -> save_instruments -> harvest_recordings -> move_to_clickhouse`). |
| `scripts/music_logic.py` | Contains ETL logic for discovery, harvesting, staging writes, and ClickHouse loading. |
| `sql/init_postgres.sh` | Bootstraps PostgreSQL schema (`instruments`, `recordings`, `harvest_progress`) and creates `airflow_meta`. |
| `sql/init_clickhouse.sql` | Bootstraps ClickHouse analytics table `instrument_trends`. |
| `docker-compose.yml` | Defines and wires services: PostgreSQL (`postgres_source`), ClickHouse (`clickhouse_warehouse`), Airflow (`airflow_orchestrator`). |
| `Dockerfile`, `requirements.txt` | Defines Python runtime and dependencies used by Airflow and ETL code. |

## Pipeline tasks and behavior

1. `scout_instruments` discovers popular instruments from MusicBrainz and returns `{instrument_name: mb_uuid}`.
2. `save_instruments` stores those targets in PostgreSQL table `instruments`.
3. `harvest_recordings` pages through recordings per instrument, extracts the earliest valid release year, captures country, and tracks checkpoints in `harvest_progress`.
4. `move_to_clickhouse` truncates and reloads ClickHouse table `instrument_trends` from PostgreSQL `recordings`.

## Data stores

| Store | Purpose | Key tables |
|---|---|---|
| PostgreSQL | Staging and resumable harvest state | `instruments`, `recordings`, `harvest_progress` |
| ClickHouse | Analytics output for trend queries | `instrument_trends` |

## Minimal run reference

- Start services: `docker compose up -d`
- Trigger DAG in Airflow UI: `http://localhost:8080` (`musicbrainz_global_pipeline`)
- Query outputs in:
  - PostgreSQL table `recordings`
  - ClickHouse table `instrument_trends`
