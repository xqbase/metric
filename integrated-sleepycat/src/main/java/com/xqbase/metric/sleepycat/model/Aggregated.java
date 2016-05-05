package com.xqbase.metric.sleepycat.model;

import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;

@Entity
public class Aggregated {
	@PrimaryKey
	public String name;
	public int time;
}