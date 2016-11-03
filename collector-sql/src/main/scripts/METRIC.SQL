CREATE TABLE metric_name (
	id INTEGER NOT NULL PRIMARY KEY AUTO_INCREMENT,
	name VARCHAR(64) NOT NULL,
	minute_size INTEGER NOT NULL DEFAULT 0,
	quarter_size INTEGER NOT NULL DEFAULT 0,
	aggregated_time INTEGER NOT NULL DEFAULT 0,
	tags LONGBLOB DEFAULT NULL,
	UNIQUE (name));

CREATE TABLE metric_minute (
	id INTEGER NOT NULL,
	time INTEGER NOT NULL,
	_count BIGINT NOT NULL,
	_sum FLOAT NOT NULL,
	_max FLOAT NOT NULL,
	_min FLOAT NOT NULL,
	_sqr FLOAT NOT NULL,
	tags LONGBLOB NOT NULL,
	KEY (id, time));

CREATE TABLE metric_quarter (
	id INTEGER NOT NULL,
	time INTEGER NOT NULL,
	_count BIGINT NOT NULL,
	_sum FLOAT NOT NULL,
	_max FLOAT NOT NULL,
	_min FLOAT NOT NULL,
	_sqr FLOAT NOT NULL,
	tags LONGBLOB NOT NULL,
	KEY (id, time));

CREATE TABLE metric_tags_quarter (
	id INTEGER NOT NULL,
	time INTEGER NOT NULL,
	tags LONGBLOB NOT NULL,
	PRIMARY KEY (id, time),
	KEY (time));