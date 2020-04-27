CREATE TABLE metric_name (
	id INTEGER NOT NULL PRIMARY KEY AUTO_INCREMENT,
	name VARCHAR(64) NOT NULL,
	minute_size LONG NOT NULL DEFAULT 0,
	quarter_size LONG NOT NULL DEFAULT 0,
	aggregated_time INTEGER NOT NULL DEFAULT 0,
	tags LONGTEXT DEFAULT NULL,
	UNIQUE (name));

CREATE TABLE metric_minute (
	id INTEGER NOT NULL,
	time INTEGER NOT NULL,
	seq INTEGER NOT NULL,
	metrics LONGTEXT NOT NULL,
	PRIMARY KEY (id, time, seq));

CREATE TABLE metric_quarter (
	id INTEGER NOT NULL,
	time INTEGER NOT NULL,
	metrics LONGTEXT NOT NULL,
	PRIMARY KEY (id, time));

CREATE TABLE metric_tags_quarter (
	id INTEGER NOT NULL,
	time INTEGER NOT NULL,
	tags LONGTEXT NOT NULL,
	PRIMARY KEY (id, time),
	KEY (time));

CREATE TABLE metric_seq (
	time INTEGER NOT NULL PRIMARY KEY,
	seq INTEGER NOT NULL);