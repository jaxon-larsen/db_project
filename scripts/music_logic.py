import os
import logging
import musicbrainzngs
import psycopg2
import time
import math
import random
import clickhouse_connect
from datetime import datetime
from contextlib import contextmanager

logger = logging.getLogger(__name__)

# Suppress verbose MusicBrainz logging to reduce memory usage
logging.getLogger('musicbrainzngs').setLevel(logging.WARNING)

musicbrainzngs.set_useragent(
    "MBProject", 
    "0.1", 
    "jaxonlarsen7@gmail.com"
)

_PG_CONN_PARAMS = {
    "host":     os.environ.get("POSTGRES_HOST", "postgres_source"),
    "database": os.environ.get("POSTGRES_DB", "musicbrainz"),
    "user":     os.environ.get("POSTGRES_USER"),
    "password": os.environ.get("POSTGRES_PASSWORD"),
}

PAGE_SIZE = 100
MIN_YEAR = 1950
MAX_YEAR = datetime.utcnow().year
DEFAULT_SAMPLE_RATIO = float(os.environ.get("COUNTRY_SAMPLE_RATIO", "0.10"))
DEFAULT_MAX_SAMPLE_PER_COUNTRY = int(os.environ.get("MAX_SAMPLE_PER_COUNTRY", "5000"))
OFFICIAL_ONLY = os.environ.get("OFFICIAL_ONLY", "false").lower() in {"1", "true", "yes"}

# Fixed candidate list for country census ranking.
CENSUS_COUNTRY_CODES = [
    "US", "GB", "DE", "FR", "JP", "CA", "AU", "IT", "ES", "BR", "SE", "NL", "NO", "DK", "FI",
    "IE", "BE", "CH", "AT", "PT", "PL", "CZ", "HU", "RO", "GR", "TR", "RU", "UA", "HR", "RS",
    "BG", "SI", "SK", "EE", "LV", "LT", "IS", "MX", "AR", "CL", "CO", "PE", "VE", "UY", "PY",
    "BO", "EC", "CR", "PA", "DO", "CU", "GT", "SV", "HN", "NI", "JM", "TT", "PR", "ZA", "NG",
    "KE", "GH", "MA", "TN", "EG", "IL", "SA", "AE", "IN", "PK", "BD", "LK", "NP", "TH", "VN",
    "MY", "SG", "ID", "PH", "KR", "CN", "TW", "HK", "NZ"
]

@contextmanager
def _pg_conn():
    """Yield a PostgreSQL connection with rollback on errors."""
    # Centralize DB connection handling for all ETL steps.
    conn = psycopg2.connect(**_PG_CONN_PARAMS)
    try:
        yield conn
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

def scout_instruments():
    """Discovers popular instruments from MusicBrainz by usage/recording count."""
    # Build a {instrument_name: MusicBrainz UUID} map for downstream tasks.
    target_count = int(os.environ.get("INSTRUMENT_COUNT", "30"))
    page_size = 100
    offset = 0
    instrument_map = {}
    
    # Optional: Prioritize specific instruments to ensure they're included
    priority_instruments = [
        "Piano", "Guitar", "Drums", "Bass", "Violin", 
        "Synthesizer", "Saxophone", "Trumpet"
    ]
    
    logger.info(f"Discovering top {target_count} popular instruments...")
    
    # First, explicitly scout priority instruments to ensure they're captured
    for inst_name in priority_instruments:
        if len(instrument_map) >= target_count:
            break
        try:
            result = musicbrainzngs.search_instruments(instrument=inst_name, limit=1)
            if result.get('instrument-list'):
                inst = result['instrument-list'][0]
                name = inst.get('name', inst_name)
                uuid = inst.get('id')
                if uuid and name not in instrument_map:
                    instrument_map[name] = uuid
                    logger.info(f"Priority instrument #{len(instrument_map)}: {name}")
        except Exception as e:
            logger.warning(f"Could not find priority instrument {inst_name}: {e}")
        finally:
            time.sleep(1)
    
    # Then discover additional popular instruments via pagination
    while len(instrument_map) < target_count:
        try:
            # Wildcard search returns instruments sorted by popularity
            result = musicbrainzngs.search_instruments(
                query="*",
                limit=page_size,
                offset=offset
            )
            
            instrument_list = result.get('instrument-list', [])
            if not instrument_list:
                logger.info("No more instruments available from MusicBrainz")
                break
            
            for inst in instrument_list:
                name = inst.get('name')
                uuid = inst.get('id')
                
                # Skip if already added or missing required fields
                if not name or not uuid or name in instrument_map:
                    continue
                
                instrument_map[name] = uuid
                logger.info(f"Discovered #{len(instrument_map)}: {name}")
                
                if len(instrument_map) >= target_count:
                    break
            
            offset += page_size
            
        except Exception as e:
            logger.error(f"Error discovering instruments at offset {offset}: {e}")
            # Don't fail completely, just stop discovery
            break
        finally:
            time.sleep(1)
    
    logger.info(f"Successfully discovered {len(instrument_map)} instruments")
    return instrument_map

def save_instruments(instrument_map):
    """Saves the scouted UUIDs into the reference table."""
    # Skip writes when there is nothing to persist.
    if not instrument_map:
        logger.warning("No instruments found to save. Instrument map is empty.")
        return
    
    with _pg_conn() as conn:
        cur = conn.cursor()
        cur.execute("TRUNCATE TABLE instruments;")
        for name, uuid in instrument_map.items():
            cur.execute(
                """
                INSERT INTO instruments (instrument_name, mb_uuid)
                VALUES (%s, %s)
                ON CONFLICT (instrument_name) DO NOTHING;
                """, (name, uuid)
            )
        conn.commit()
        cur.close()
    logger.info(f"Successfully saved {len(instrument_map)} instruments to Postgres.")

def _escape_query_value(value):
    return value.replace('"', '\\"')

def _qualified_country_query(country_code, instrument_name=None, official_only=OFFICIAL_ONLY):
    query_parts = [
        f"country:{country_code}",
        "has_instrument:true",
    ]
    if official_only:
        query_parts.append("status:official")
    if instrument_name:
        query_parts.append(f'instrument:"{_escape_query_value(instrument_name)}"')
    return " AND ".join(query_parts)

def _count_recordings(query):
    try:
        result = musicbrainzngs.search_recordings(query=query, limit=1, offset=0)
        count_value = result.get("recording-count", 0)
        return int(count_value)
    except Exception as e:
        logger.error(f"Count query failed for '{query}': {e}")
        return 0
    finally:
        time.sleep(1)

def _extract_earliest_release(rec, target_country):
    years = []
    countries = []
    for release in rec.get("release-list", []):
        date_str = release.get("date", "")
        if date_str and len(date_str) >= 4:
            try:
                year = int(date_str[:4])
                if MIN_YEAR <= year <= MAX_YEAR:
                    years.append(year)
            except ValueError:
                pass
        country = release.get("country")
        if country:
            countries.append(country)

    if not years:
        return None, None

    # Prefer matching country from release metadata when available.
    country_code = target_country if target_country in countries else (countries[0] if countries else target_country)
    return min(years), country_code

def reset_pipeline_data():
    """Clears staging and warehouse tables before a fresh randomized sample run."""
    with _pg_conn() as conn:
        cur = conn.cursor()
        cur.execute("TRUNCATE TABLE recordings;")
        cur.execute("TRUNCATE TABLE harvest_progress;")
        conn.commit()
        cur.close()

    ch_client = clickhouse_connect.get_client(
        host='clickhouse_warehouse',
        port=8123,
        username='default',
        password=os.environ.get('CLICKHOUSE_PASSWORD', '')
    )
    try:
        ch_client.command('TRUNCATE TABLE instrument_trends')
    finally:
        ch_client.close()

    logger.info("Reset complete: Postgres recordings/harvest_progress and ClickHouse instrument_trends truncated.")

def census_top_countries(top_n=10):
    """Ranks countries by qualified recording volume and returns top country codes."""
    country_counts = []
    for code in CENSUS_COUNTRY_CODES:
        query = _qualified_country_query(code)
        count = _count_recordings(query)
        if count > 0:
            country_counts.append((code, count))
            logger.info(f"Census count {code}: {count}")

    country_counts.sort(key=lambda row: row[1], reverse=True)
    top_countries = [code for code, _ in country_counts[:top_n]]
    logger.info(f"Selected top countries: {top_countries}")
    return top_countries

def census_top_country_args(top_n=10):
    """Returns top countries as mapped PythonOperator op_args payload."""
    return [[code] for code in census_top_countries(top_n=top_n)]

def harvest_country_recordings(country_code):
    """Harvests random-offset samples for one country and writes staged rows to Postgres."""
    with _pg_conn() as conn:
        cur = conn.cursor()
        cur.execute("SELECT instrument_name, mb_uuid FROM instruments;")
        scouted = cur.fetchall()
        cur.close()

    if not scouted:
        logger.warning("No instruments found in instruments table. Country harvest skipped.")
        return 0

    country_total = _count_recordings(_qualified_country_query(country_code))
    if country_total == 0:
        logger.warning(f"No qualified recordings for country {country_code}.")
        return 0

    target_rows = min(int(country_total * DEFAULT_SAMPLE_RATIO), DEFAULT_MAX_SAMPLE_PER_COUNTRY)
    target_rows = max(target_rows, 1)
    per_instrument_target = max(1, math.ceil(target_rows / len(scouted)))
    rng = random.Random(f"{country_code}:{country_total}")

    logger.info(
        f"{country_code}: total qualified={country_total}, target_rows={target_rows}, "
        f"instruments={len(scouted)}, per_instrument_target={per_instrument_target}"
    )

    country_kept = 0
    with _pg_conn() as conn:
        cur = conn.cursor()
        for inst_name, _ in scouted:
            if country_kept >= target_rows:
                break

            query = _qualified_country_query(country_code, instrument_name=inst_name)
            inst_count = _count_recordings(query)
            if inst_count == 0:
                continue

            total_pages = math.ceil(inst_count / PAGE_SIZE)
            page_indexes = list(range(total_pages))
            rng.shuffle(page_indexes)

            kept_for_instrument = 0
            for page_index in page_indexes:
                if kept_for_instrument >= per_instrument_target or country_kept >= target_rows:
                    break

                offset = page_index * PAGE_SIZE
                try:
                    result = musicbrainzngs.search_recordings(query=query, limit=PAGE_SIZE, offset=offset)
                except Exception as e:
                    logger.error(f"Harvest query failed for {country_code}/{inst_name} at offset {offset}: {e}")
                    time.sleep(1)
                    continue
                finally:
                    time.sleep(1)

                recording_list = result.get("recording-list", [])
                if not recording_list:
                    continue

                batch_data = []
                for rec in recording_list:
                    if kept_for_instrument >= per_instrument_target or country_kept >= target_rows:
                        break

                    rec_id = rec.get("id")
                    rec_title = rec.get("title", "Unknown")
                    if not rec_id:
                        continue

                    earliest_year, release_country = _extract_earliest_release(rec, country_code)
                    if earliest_year is None or not release_country:
                        continue

                    batch_data.append((rec_id, inst_name, rec_title, earliest_year, release_country))
                    kept_for_instrument += 1
                    country_kept += 1

                if batch_data:
                    cur.executemany(
                        """
                        INSERT INTO recordings (recording_id, instrument_name, recording_name, release_year, country_code)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (recording_id, instrument_name) DO NOTHING
                        """,
                        batch_data
                    )
                    conn.commit()

            logger.info(f"{country_code}/{inst_name}: kept {kept_for_instrument}")

        cur.close()

    logger.info(f"{country_code}: completed with {country_kept} staged rows.")
    return country_kept

def harvest_recordings(country_codes):
    """Harvests recordings by country list returned from census task."""
    if not country_codes:
        logger.warning("No country codes provided for harvest.")
        return

    total_rows = 0
    for code in country_codes:
        total_rows += harvest_country_recordings(code)
    logger.info(f"Country harvest complete. Total staged rows: {total_rows}")

def move_to_clickhouse():
    """Moves harvested data from Postgres to ClickHouse for analysis."""
    # Rebuild analytics table contents from the latest Postgres staging data.
    ch_client = None
    try:
        ch_client = clickhouse_connect.get_client(
            host='clickhouse_warehouse', 
            port=8123, 
            username='default',
            password=os.environ.get('CLICKHOUSE_PASSWORD', '')
        )
        
        # Truncate ClickHouse table for clean state
        logger.info("Truncating ClickHouse table for fresh load...")
        ch_client.command('TRUNCATE TABLE instrument_trends')
        
        with _pg_conn() as conn:
            cur = conn.cursor()
            
            # Get total count for logging (all instrument-recording pairs)
            cur.execute("SELECT COUNT(*) FROM recordings;")
            total_rows = cur.fetchone()[0]
            
            if total_rows == 0:
                logger.warning("No data to move to ClickHouse.")
                cur.close()
                return
            
            logger.info(f"Moving {total_rows} instrument-recording pairs to ClickHouse in batches...")
            
            # Move all instrument-recording pairs (not just unique recordings)
            cur.execute("""
                SELECT instrument_name, recording_name, release_year, country_code 
                FROM recordings 
                ORDER BY instrument_name, release_year;
            """)
            
            batch_size = 1000
            rows_inserted = 0
            
            while True:
                batch = cur.fetchmany(batch_size)
                if not batch:
                    break
                
                ch_client.insert('instrument_trends', batch, 
                               column_names=['instrument', 'recording_name', 'release_year', 'country_code'])
                rows_inserted += len(batch)
                logger.info(f"Inserted batch: {rows_inserted}/{total_rows} rows")
            
            cur.close()
            logger.info(f"Successfully moved {rows_inserted} rows to ClickHouse!")
    
    finally:
        if ch_client:
            ch_client.close()
            logger.info("ClickHouse connection closed.")
