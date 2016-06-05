package com.xqbase.metric.collector;

import java.util.HashMap;

public class MetricRow {
	public int time;
	public long count;
	public double sum, max, min, sqr;
	public HashMap<String, String> tags;
}