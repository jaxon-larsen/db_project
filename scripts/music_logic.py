import os
import logging
import musicbrainzngs
import psycopg2
import time
import clickhouse_connect
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

def harvest_recordings():
    """Fetches recordings using 'Earliest Release' logic with pagination and checkpointing."""
    # Iterate through instruments and persist cleaned recording rows with checkpoints.
    MAX_RECORDINGS_PER_INSTRUMENT = 2000  # Limit per instrument
    PAGE_SIZE = 100
    MIN_YEAR = 1900
    MAX_YEAR = 2026
    
    # Initialize tables
    with _pg_conn() as conn:
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS recordings (
                recording_id    TEXT NOT NULL,
                instrument_name TEXT NOT NULL,
                recording_name  TEXT,
                release_year    INT,
                country_code    TEXT,
                PRIMARY KEY (recording_id, instrument_name)
            );
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS harvest_progress (
                instrument_name TEXT PRIMARY KEY,
                recordings_fetched INT DEFAULT 0,
                last_offset INT DEFAULT 0,
                completed BOOLEAN DEFAULT FALSE,
                last_updated TIMESTAMP DEFAULT NOW()
            );
        """)
        conn.commit()
        cur.execute("SELECT instrument_name, mb_uuid FROM instruments;")
        scouted = cur.fetchall()
        cur.close()
        
    if not scouted:
        logger.warning("No instruments found in instruments table. Skipping harvest.")
        return

    logger.info(f"Found {len(scouted)} instruments to harvest (max {MAX_RECORDINGS_PER_INSTRUMENT} per instrument).")
    
    for inst_name, inst_uuid in scouted:
        # Use a fresh connection for each instrument to avoid long-lived connections
        with _pg_conn() as conn:
            cur = conn.cursor()
            # Check if already completed
            cur.execute("""
                SELECT recordings_fetched, last_offset, completed 
                FROM harvest_progress 
                WHERE instrument_name = %s
            """, (inst_name,))
            progress = cur.fetchone()
            
            if progress and progress[2]:  # completed = True
                logger.info(f"Skipping {inst_name} - already completed with {progress[0]} recordings.")
                continue
            
            start_offset = progress[1] if progress else 0
            total_fetched = progress[0] if progress else 0
            
            logger.info(f"Harvesting {inst_name} starting at offset {start_offset}...")
            
            offset = start_offset
            consecutive_errors = 0
            MAX_CONSECUTIVE_ERRORS = 3
            
            while offset < MAX_RECORDINGS_PER_INSTRUMENT:
                query = f"iid:{inst_uuid}"
                
                try:
                    result = musicbrainzngs.search_recordings(
                        query=query, 
                        limit=PAGE_SIZE, 
                        offset=offset
                    )
                    
                    # Reset error counter on success
                    consecutive_errors = 0
                    
                    recording_list = result.get('recording-list', [])
                    if not recording_list:
                        logger.info(f"No more recordings found for {inst_name} at offset {offset}")
                        break
                    
                    # Batch insert recordings (with early filtering)
                    batch_data = []
                    skipped_count = 0
                    for rec in recording_list:
                        rec_id = rec.get('id')
                        rec_title = rec.get('title', 'Unknown')
                        
                        # Find the earliest year across all releases
                        years = []
                        countries = []
                        for release in rec.get('release-list', []):
                            date_str = release.get('date', '')
                            if date_str and len(date_str) >= 4:
                                try:
                                    year = int(date_str[:4])
                                    if MIN_YEAR <= year <= MAX_YEAR:
                                        years.append(year)
                                except ValueError:
                                    pass
                            
                            # Get country from release
                            country = release.get('country', None)
                            if country:
                                countries.append(country)
                        
                        earliest_year = min(years) if years else None
                        # Use first country found, or None
                        country_code = countries[0] if countries else None
                        
                        # Early filtering: skip if missing year or country
                        if earliest_year is None or country_code is None:
                            skipped_count += 1
                            continue
                        
                        batch_data.append((rec_id, inst_name, rec_title, earliest_year, country_code))
                    
                    # Bulk insert with ON CONFLICT to handle duplicates
                    if batch_data:
                        cur.executemany(
                            """
                            INSERT INTO recordings (recording_id, instrument_name, recording_name, release_year, country_code)
                            VALUES (%s, %s, %s, %s, %s)
                            ON CONFLICT (recording_id, instrument_name) DO NOTHING
                            """, batch_data
                        )
                        conn.commit()
                    
                    fetched_count = len(batch_data)  # Only count kept recordings
                    total_fetched += fetched_count
                    offset += PAGE_SIZE
                    
                    # Update progress checkpoint
                    cur.execute("""
                        INSERT INTO harvest_progress (instrument_name, recordings_fetched, last_offset, completed, last_updated)
                        VALUES (%s, %s, %s, %s, NOW())
                        ON CONFLICT (instrument_name) 
                        DO UPDATE SET 
                            recordings_fetched = EXCLUDED.recordings_fetched,
                            last_offset = EXCLUDED.last_offset,
                            completed = EXCLUDED.completed,
                            last_updated = NOW()
                    """, (inst_name, total_fetched, offset, False))
                    conn.commit()
                    
                    logger.info(f"{inst_name}: kept {fetched_count}, skipped {skipped_count} (total: {total_fetched}, offset: {offset})")
                    
                    # Stop if we got less than PAGE_SIZE from API (no more results)
                    if len(recording_list) < PAGE_SIZE:
                        logger.info(f"Reached end of results for {inst_name}")
                        break
                
                except Exception as e:
                    consecutive_errors += 1
                    logger.error(f"Error harvesting {inst_name} at offset {offset}: {e} (attempt {consecutive_errors}/{MAX_CONSECUTIVE_ERRORS})")
                    
                    if consecutive_errors >= MAX_CONSECUTIVE_ERRORS:
                        logger.error(f"Too many consecutive errors for {inst_name}, moving to next instrument")
                        break
                    
                    # Wait longer before retry on network errors
                    time.sleep(3)
                    continue  # Retry same offset
                
                finally:
                    # Only sleep if we didn't just error and sleep already
                    if consecutive_errors == 0:
                        time.sleep(1)  # Rate limiting
            
            # Mark as completed
            cur.execute("""
                UPDATE harvest_progress 
                SET completed = TRUE, last_updated = NOW()
                WHERE instrument_name = %s
            """, (inst_name,))
            conn.commit()
            cur.close()
            
            logger.info(f"Completed {inst_name}: {total_fetched} total recordings harvested")
    
    logger.info("Harvesting complete!")

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
