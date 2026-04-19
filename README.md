# MusicBrainz Data Pipeline

This project builds a repeatable instrument-trend dataset from MusicBrainz. It discovers instruments, harvests recording metadata, stages data in PostgreSQL, and serves analytics-ready rows from ClickHouse.

## End-to-end flow

`MusicBrainz API -> PostgreSQL (staging) -> ClickHouse (analytics)`

Airflow orchestrates this flow with the DAG `musicbrainz_global_pipeline`.

## What each project part does

| Path | Responsibility |
|---|---|
| `dags/main_dag.py` | Defines pipeline task order, retries, and dependencies (`scout_instruments -> save_instruments -> reset_data -> census_countries -> harvest_recordings -> move_to_clickhouse`). |
| `scripts/music_logic.py` | Contains ETL logic for discovery, harvesting, staging writes, and ClickHouse loading. |
| `sql/init_postgres.sh` | Bootstraps PostgreSQL schema (`instruments`, `recordings`, `harvest_progress`) and creates `airflow_meta`. |
| `sql/init_clickhouse.sql` | Bootstraps ClickHouse analytics table `instrument_trends`. |
| `docker-compose.yml` | Defines and wires services: PostgreSQL (`postgres_source`), ClickHouse (`clickhouse_warehouse`), Airflow (`airflow_orchestrator`). |
| `Dockerfile`, `requirements.txt` | Defines Python runtime and dependencies used by Airflow and ETL code. |

## Pipeline tasks and behavior

1. `scout_instruments` discovers popular instruments from MusicBrainz and returns `{instrument_name: mb_uuid}`.
2. `save_instruments` refreshes PostgreSQL table `instruments` with the latest scouted set (table is truncated then reloaded).
3. `reset_data` truncates Postgres staging (`recordings`, `harvest_progress`) and ClickHouse output (`instrument_trends`) to remove legacy data before sampling.
4. `census_countries` ranks a fixed ISO country candidate list by MusicBrainz qualified recording counts (`country:<CODE> AND has_instrument:true`, plus optional `status:official` when `OFFICIAL_ONLY=true`) and returns top 10 countries.
5. `harvest_recordings` performs stratified random offset sampling per selected country and instrument, targeting `min(10% of qualified country recordings, 5000)` rows per country, with earliest-year extraction bounded to 1950-present.
6. `move_to_clickhouse` truncates and reloads ClickHouse table `instrument_trends` from PostgreSQL `recordings`.

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
