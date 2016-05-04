package com.xqbase.metric.sleepycat.model;

import java.util.Collection;
import java.util.HashMap;

import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;

@Entity
public class Tags {
	@PrimaryKey
	public String name;
	public HashMap<String, Collection<?>> tags;
}