CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;

DROP TABLE IF EXISTS events;
DROP TABLE IF EXISTS bool_values;
DROP TABLE IF EXISTS float_values;
DROP TABLE IF EXISTS int_values;
DROP TABLE IF EXISTS string_values;

CREATE TABLE events(
	"time"  TIMESTAMP WITH TIME ZONE NOT NULL,
	"conditionname"  VARCHAR(32) NOT NULL,
	"message" TEXT NOT NULL,
	"sourcenode" VARCHAR(64) NOT NULL,
	"sourcename" VARCHAR(34) NOT NULL,
	"ack_transitiontime" TIMESTAMP WITH TIME ZONE DEFAULT NULL
);

CREATE TABLE bool_values (
	"sourcetimestamp"  TIMESTAMP WITH TIME ZONE NOT NULL,
	"nodeid" TEXT NOT NULL,
	"status" INTEGER NOT NULL,
	"value" BOOLEAN DEFAULT NULL
);

CREATE TABLE float_values (
	"sourcetimestamp"  TIMESTAMP WITH TIME ZONE NOT NULL,
	"nodeid" TEXT NOT NULL,
	"status" INTEGER NOT NULL,
	"value" DOUBLE PRECISION DEFAULT NULL
);

CREATE TABLE int_values (
	"sourcetimestamp"  TIMESTAMP WITH TIME ZONE NOT NULL,
	"nodeid" TEXT NOT NULL,
	"status" INTEGER NOT NULL,
	"value" INTEGER DEFAULT NULL
);

CREATE TABLE string_values (
	"sourcetimestamp"  TIMESTAMP WITH TIME ZONE NOT NULL,
	"nodeid" TEXT NOT NULL,
	"status" INTEGER NOT NULL,
	"value" VARCHAR(256) DEFAULT NULL
);

SELECT create_hypertable('events ', 'time');
SELECT create_hypertable('bool_values ', 'sourcetimestamp');
SELECT create_hypertable('float_values ', 'sourcetimestamp');
SELECT create_hypertable('int_values ', 'sourcetimestamp');
SELECT create_hypertable('string_values ', 'sourcetimestamp');

SELECT set_chunk_time_interval('events ', chunk_time_interval => INTERVAL '10 year');
SELECT set_chunk_time_interval('bool_values ', chunk_time_interval => INTERVAL '10 year');
SELECT set_chunk_time_interval('float_values ', chunk_time_interval => INTERVAL '10 year');
SELECT set_chunk_time_interval('int_values ', chunk_time_interval => INTERVAL '10 year');
SELECT set_chunk_time_interval('string_values ', chunk_time_interval => INTERVAL '10 year');

ALTER TABLE events  SET (
  timescaledb.compress,
  timescaledb.compress_orderby = 'time DESC',
  timescaledb.compress_segmentby = 'sourcenode');
ALTER TABLE bool_values  SET (
  timescaledb.compress,
  timescaledb.compress_orderby = 'sourcetimestamp DESC',
  timescaledb.compress_segmentby = 'nodeid');
ALTER TABLE float_values  SET (
  timescaledb.compress,
  timescaledb.compress_orderby = 'sourcetimestamp DESC',
  timescaledb.compress_segmentby = 'nodeid');
ALTER TABLE int_values  SET (
  timescaledb.compress,
  timescaledb.compress_orderby = 'sourcetimestamp DESC',
  timescaledb.compress_segmentby = 'nodeid');
ALTER TABLE string_values  SET (
  timescaledb.compress,
  timescaledb.compress_orderby = 'sourcetimestamp DESC',
  timescaledb.compress_segmentby = 'nodeid');
