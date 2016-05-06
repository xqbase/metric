package com.xqbase.metric.sleepycat.model;

import com.sleepycat.persist.model.Persistent;

@Persistent
public class TagValue {
	public String value;
	public long count;
	public double sum, max, min, sqr;
}