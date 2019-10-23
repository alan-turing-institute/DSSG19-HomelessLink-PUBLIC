-- ======
-- Description:
-- 	1. Creates extensions for postgis
-- Last updated: August 29, 2019
-- ======

-- enable GIS
create extension postgis;

-- enable topology
create extension postgis_topology;

-- enable postgis advanced 3d and other geoprocessing algorithms
create extension postgis_sfcgal;

-- enable routing algorithms
create extension pgrouting;

-- enable advanced text manipulation
create extension if not exists fuzzystrmatch;
create extension if not exists unaccent;
create extension if not exists pg_trgm;

-- enable bloom filters (for fast counting)
create extension if not exists bloom;

create extension if not exists citext;

-- calculate distance in multidimensional data
create extension if not exists cube;

-- enable files as tables, postgres db as tables
create extension if not exists file_fdw;
create extension if not exists postgres_fdw;

-- enable calculation between lat/long (without using GIS)
create extension if not exists earthdistance;
