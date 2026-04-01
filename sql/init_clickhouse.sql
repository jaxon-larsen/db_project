CREATE TABLE IF NOT EXISTS instrument_trends (
    instrument      String,
    recording_name  String,
    release_year    UInt16,
    country_code    String
) ENGINE = MergeTree()
ORDER BY (instrument, release_year);
