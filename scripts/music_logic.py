import os
import logging
import musicbrainzngs
import psycopg2
import time
import clickhouse_connect
from contextlib import contextmanager

logger = logging.getLogger(__name__)

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
    conn = psycopg2.connect(**_PG_CONN_PARAMS)
    try:
        yield conn
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

def scout_instruments():
    """Finds UUIDs for target instruments."""
    target_instruments = ["Sitar", "Synthesizer", "Bagpipes", "Electric Guitar"]
    instrument_map = {}
    
    logger.info(f"Scouting {len(target_instruments)} instruments...")
    for instrument in target_instruments:
        try:
            result = musicbrainzngs.search_instruments(instrument=instrument, limit=1)
            if result['instrument-list']:
                inst_data = result['instrument-list'][0]
                instrument_map[instrument] = inst_data['id']
                logger.info(f"Found {instrument}: {inst_data['id']}")
        except Exception as e:
            logger.error(f"Error scouting {instrument}: {e}")
            raise
        finally:
            time.sleep(1)
    return instrument_map

def save_instruments(instrument_map):
    """Saves the scouted UUIDs into the reference table."""
    if not instrument_map:
        logger.warning("No instruments found to save. Instrument map is empty.")
        return
    
    with _pg_conn() as conn:
        cur = conn.cursor()
        for name, uuid in instrument_map.items():
            cur.execute(
                """
                INSERT INTO target_instruments (instrument_name, mb_uuid)
                VALUES (%s, %s)
                ON CONFLICT (instrument_name) DO NOTHING;
                """, (name, uuid)
            )
        conn.commit()
        cur.close()
    logger.info(f"Successfully saved {len(instrument_map)} instruments to Postgres.")

def harvest_recordings():
    """Fetches recordings using 'Earliest Release' logic."""
    countries = ['US', 'GB', 'IN', 'JP', 'BR']
    
    with _pg_conn() as conn:
        cur = conn.cursor()

        # Create table if it doesn't exist, then truncate to prevent duplicates on re-runs
        cur.execute("""
            CREATE TABLE IF NOT EXISTS recording_data (
                instrument_name TEXT,
                recording_name  TEXT,
                release_year    INT,
                country_code    TEXT
            );
        """)
        cur.execute("TRUNCATE recording_data;")
        conn.commit()

        cur.execute("SELECT instrument_name, mb_uuid FROM target_instruments;")
        scouted = cur.fetchall()
        
        if not scouted:
            logger.warning("No instruments found in target_instruments table. Skipping harvest.")
            cur.close()
            return

        logger.info(f"Found {len(scouted)} instruments to harvest.")
        for inst_name, inst_uuid in scouted:
            for country in countries:
                logger.info(f"Harvesting {inst_name} in {country}...")
                query = f"iid:{inst_uuid} AND country:{country}"
                try:
                    result = musicbrainzngs.search_recordings(query=query, limit=50)

                    for rec in result.get('recording-list', []):
                        # Find the earliest year across all releases
                        years = []
                        for release in rec.get('release-list', []):
                            date_str = release.get('date', '')
                            if date_str and len(date_str) >= 4:
                                try:
                                    years.append(int(date_str[:4]))
                                except ValueError:
                                    continue
                        
                        earliest_year = min(years) if years else None

                        cur.execute(
                            """
                            INSERT INTO recording_data (instrument_name, recording_name, release_year, country_code)
                            VALUES (%s, %s, %s, %s)
                            """, (inst_name, rec['title'], earliest_year, country)
                        )
                    conn.commit()
                except Exception as e:
                    logger.error(f"Harvesting error for {inst_name} in {country}: {e}")
                    raise
                finally:
                    time.sleep(1)

        cur.close()
    logger.info("Harvesting complete!")

def move_to_clickhouse():
    """Moves harvested data from Postgres to ClickHouse for analysis."""
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
        ch_client.command('TRUNCATE TABLE global_instrument_trends')
        
        with _pg_conn() as conn:
            cur = conn.cursor()
            
            # Get total count for logging
            cur.execute("SELECT COUNT(*) FROM recording_data WHERE release_year IS NOT NULL;")
            total_rows = cur.fetchone()[0]
            
            if total_rows == 0:
                logger.warning("No data with valid release_year to move to ClickHouse.")
                cur.close()
                return
            
            logger.info(f"Moving {total_rows} rows to ClickHouse in batches...")
            
            # Use server-side cursor for batched fetching
            cur.execute("""
                SELECT instrument_name, recording_name, release_year, country_code 
                FROM recording_data 
                WHERE release_year IS NOT NULL;
            """)
            
            batch_size = 1000
            rows_inserted = 0
            
            while True:
                batch = cur.fetchmany(batch_size)
                if not batch:
                    break
                
                ch_client.insert('global_instrument_trends', batch, 
                               column_names=['instrument', 'recording_name', 'release_year', 'country_code'])
                rows_inserted += len(batch)
                logger.info(f"Inserted batch: {rows_inserted}/{total_rows} rows")
            
            cur.close()
            logger.info(f"Successfully moved {rows_inserted} rows to ClickHouse!")
    
    finally:
        if ch_client:
            ch_client.close()
            logger.info("ClickHouse connection closed.")

