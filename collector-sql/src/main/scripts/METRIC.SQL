CREATE TABLE metric_aggregated (
	name VARCHAR(64) NOT NULL PRIMARY KEY,
	time INTEGER NOT NULL);

CREATE TABLE metric_minute (
	name VARCHAR(64) NOT NULL,
	time INTEGER NOT NULL,
	_count LONG NOT NULL,
	_sum FLOAT NOT NULL,
	_max FLOAT NOT NULL,
	_min FLOAT NOT NULL,
	_sqr FLOAT NOT NULL,
	tags LONGBLOB NOT NULL,
	KEY (name, time));

CREATE TABLE metric_minute_size (
	name VARCHAR(64) NOT NULL PRIMARY KEY,
	size INTEGER NOT NULL);

CREATE TABLE metric_quarter (
	name VARCHAR(64) NOT NULL,
	time INTEGER NOT NULL,
	_count LONG NOT NULL,
	_sum FLOAT NOT NULL,
	_max FLOAT NOT NULL,
	_min FLOAT NOT NULL,
	_sqr FLOAT NOT NULL,
	tags LONGBLOB NOT NULL,
	KEY (name, time));

CREATE TABLE metric_quarter_size (
	name VARCHAR(64) NOT NULL PRIMARY KEY,
	size INTEGER NOT NULL);

CREATE TABLE metric_tags_all (
	name VARCHAR(64) NOT NULL PRIMARY KEY,
	tags LONGBLOB NOT NULL);

CREATE TABLE metric_tags_quarter (
	name VARCHAR(64) NOT NULL,
	time INTEGER NOT NULL,
	tags LONGBLOB NOT NULL,
	PRIMARY KEY (name, time),
	KEY (time));