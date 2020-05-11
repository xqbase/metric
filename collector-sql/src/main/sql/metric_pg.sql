CREATE TABLE metric_name (
	id SERIAL PRIMARY KEY,
	name VARCHAR(64) NOT NULL,
	minute_size BIGINT NOT NULL DEFAULT 0,
	quarter_size BIGINT NOT NULL DEFAULT 0,
	aggregated_time INTEGER NOT NULL DEFAULT 0,
	tags TEXT DEFAULT NULL,
	UNIQUE (name));

CREATE TABLE metric_minute (
	id INTEGER NOT NULL,
	time INTEGER NOT NULL,
	seq INTEGER NOT NULL,
	metrics TEXT NOT NULL,
	PRIMARY KEY (id, time, seq));

CREATE TABLE metric_quarter (
	id INTEGER NOT NULL,
	time INTEGER NOT NULL,
	metrics TEXT NOT NULL,
	PRIMARY KEY (id, time));

CREATE TABLE metric_tags_quarter (
	id INTEGER NOT NULL,
	time INTEGER NOT NULL,
	tags TEXT NOT NULL,
	PRIMARY KEY (id, time));

CREATE TABLE metric_seq (
	time INTEGER NOT NULL PRIMARY KEY,
	seq INTEGER NOT NULL);