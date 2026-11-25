-- PostgreSQL 17 logical replication schema for Debezium CDC catalog source
CREATE TABLE IF NOT EXISTS public.content_catalog (
    item_id TEXT PRIMARY KEY,
    title TEXT NOT NULL,
    genre TEXT NOT NULL,
    duration_seconds INTEGER NOT NULL CHECK (duration_seconds > 0),
    release_year INTEGER NOT NULL,
    maturity_rating TEXT NOT NULL,
    popularity_prior DOUBLE PRECISION NOT NULL CHECK (popularity_prior >= 0.0 AND popularity_prior <= 1.0),
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

ALTER TABLE public.content_catalog REPLICA IDENTITY FULL;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = 'recsys_catalog_pub') THEN
        CREATE PUBLICATION recsys_catalog_pub FOR TABLE public.content_catalog;
    END IF;
END $$;
