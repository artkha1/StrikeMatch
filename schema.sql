CREATE EXTENSION IF NOT EXISTS postgis;

CREATE TABLE IF NOT EXISTS firms_detections (
    id           BIGSERIAL                  PRIMARY KEY,
    acq_datetime TIMESTAMPTZ                NOT NULL,
    geom         GEOGRAPHY(POINT, 4326)     NOT NULL,
    latitude     DOUBLE PRECISION           NOT NULL,
    longitude    DOUBLE PRECISION           NOT NULL,
    bright_ti4   REAL,
    bright_ti5   REAL,
    frp          REAL,
    scan         REAL,
    track        REAL,
    satellite    VARCHAR(10),
    confidence   VARCHAR(10),
    daynight     CHAR(1),
    type         SMALLINT,
    version      VARCHAR(10),
    ingested_at  TIMESTAMPTZ                NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS firms_detections_geom_idx
    ON firms_detections USING GIST (geom);

CREATE INDEX IF NOT EXISTS firms_detections_acq_datetime_idx
    ON firms_detections (acq_datetime);

-- Idempotent migration: rename gdelt_events -> acled_events if upgrading from old schema.
-- Also migrates global_event_id BIGINT->TEXT and renames the FK column in fire_event_correlations.
DO $$
BEGIN
    -- Rename table if coming from GDELT schema
    IF EXISTS (SELECT 1 FROM pg_tables WHERE tablename = 'gdelt_events' AND schemaname = 'public')
       AND NOT EXISTS (SELECT 1 FROM pg_tables WHERE tablename = 'acled_events' AND schemaname = 'public')
    THEN
        ALTER TABLE gdelt_events RENAME TO acled_events;
    END IF;

    -- Change global_event_id BIGINT -> TEXT (ACLED uses string IDs like "UKR12345")
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'acled_events'
          AND column_name = 'global_event_id'
          AND data_type = 'bigint'
    ) THEN
        ALTER TABLE acled_events ALTER COLUMN global_event_id TYPE TEXT USING global_event_id::TEXT;
    END IF;

    -- Drop NOT NULL constraints on GDELT-only columns so ACLED inserts can omit them
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'acled_events' AND column_name = 'cameo_code'
    ) THEN
        ALTER TABLE acled_events ALTER COLUMN cameo_code DROP NOT NULL;
    END IF;
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'acled_events' AND column_name = 'action_geo_type'
    ) THEN
        ALTER TABLE acled_events ALTER COLUMN action_geo_type DROP NOT NULL;
    END IF;

    -- Rename FK column in fire_event_correlations and update the constraint
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'fire_event_correlations' AND column_name = 'gdelt_event_id'
    ) THEN
        ALTER TABLE fire_event_correlations
            DROP CONSTRAINT IF EXISTS fire_event_correlations_gdelt_event_id_fkey;
        ALTER TABLE fire_event_correlations RENAME COLUMN gdelt_event_id TO acled_event_id;
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.table_constraints
            WHERE constraint_name = 'fire_event_correlations_acled_event_id_fkey'
              AND table_name = 'fire_event_correlations'
        ) THEN
            ALTER TABLE fire_event_correlations
                ADD CONSTRAINT fire_event_correlations_acled_event_id_fkey
                FOREIGN KEY (acled_event_id) REFERENCES acled_events(id);
        END IF;
    END IF;
END $$;

-- ACLED strike events (replaces gdelt_events)
CREATE TABLE IF NOT EXISTS acled_events (
    id                  BIGSERIAL                  PRIMARY KEY,
    global_event_id     TEXT                       NOT NULL UNIQUE,  -- ACLED event_id_cnty
    event_date          DATE                       NOT NULL,
    event_datetime      TIMESTAMPTZ                NOT NULL,
    event_type          TEXT,
    sub_event_type      TEXT,
    description         TEXT,                      -- ACLED notes field
    num_sources         INTEGER,
    actor1_name         TEXT,
    actor2_name         TEXT,
    action_geo_fullname TEXT,
    action_geo_country  TEXT,
    fatalities          INTEGER,
    geom                GEOGRAPHY(POINT, 4326)     NOT NULL,
    latitude            DOUBLE PRECISION           NOT NULL,
    longitude           DOUBLE PRECISION           NOT NULL,
    source_url          TEXT,
    ingested_at         TIMESTAMPTZ                NOT NULL DEFAULT NOW()
);

-- Idempotent column additions for tables upgraded via the DO block above
ALTER TABLE acled_events ADD COLUMN IF NOT EXISTS event_type TEXT;
ALTER TABLE acled_events ADD COLUMN IF NOT EXISTS sub_event_type TEXT;
ALTER TABLE acled_events ADD COLUMN IF NOT EXISTS description TEXT;
ALTER TABLE acled_events ADD COLUMN IF NOT EXISTS fatalities INTEGER;

CREATE INDEX IF NOT EXISTS acled_events_geom_idx
    ON acled_events USING GIST (geom);
CREATE INDEX IF NOT EXISTS acled_events_datetime_idx
    ON acled_events (event_datetime);
CREATE INDEX IF NOT EXISTS acled_events_subtype_idx
    ON acled_events (sub_event_type);

-- Scored FIRMS × ACLED correlation pairs
CREATE TABLE IF NOT EXISTS fire_event_correlations (
    id                  BIGSERIAL   PRIMARY KEY,
    firms_detection_id  BIGINT      NOT NULL REFERENCES firms_detections(id),
    acled_event_id      BIGINT      NOT NULL REFERENCES acled_events(id),
    distance_m          REAL        NOT NULL,
    time_delta_h        REAL        NOT NULL,   -- negative = event occurred before fire detected
    score               REAL        NOT NULL,
    infra_id            BIGINT,                 -- FK reserved for Phase 3; NULL in Phase 2
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (firms_detection_id, acled_event_id)
);

CREATE INDEX IF NOT EXISTS fec_firms_idx
    ON fire_event_correlations (firms_detection_id);
CREATE INDEX IF NOT EXISTS fec_acled_idx
    ON fire_event_correlations (acled_event_id);
CREATE INDEX IF NOT EXISTS fec_score_idx
    ON fire_event_correlations (score DESC);

-- 14-day deduplicated FIRMS snapshot, regenerated by spark_pipeline.py.
-- id mirrors firms_detections.id; geom is populated by psycopg2 after JDBC write.
CREATE TABLE IF NOT EXISTS firms_silver (
    id           BIGINT                     PRIMARY KEY,
    acq_datetime TIMESTAMPTZ                NOT NULL,
    geom         GEOGRAPHY(POINT, 4326),
    latitude     DOUBLE PRECISION           NOT NULL,
    longitude    DOUBLE PRECISION           NOT NULL,
    bright_ti4   REAL,
    bright_ti5   REAL,
    frp          REAL,
    scan         REAL,
    track        REAL,
    satellite    VARCHAR(10),
    confidence   VARCHAR(10),
    daynight     CHAR(1),
    type         SMALLINT,
    version      VARCHAR(10),
    ingested_at  TIMESTAMPTZ                NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS firms_silver_geom_idx
    ON firms_silver USING GIST (geom);

CREATE INDEX IF NOT EXISTS firms_silver_acq_datetime_idx
    ON firms_silver (acq_datetime);
