package com.xqbase.metric.sleepycat.model;

import java.util.ArrayList;
import java.util.HashMap;

import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;

@Entity
public class AllTags {
	@PrimaryKey
	public String name;
	public HashMap<String, ArrayList<TagValue>> tags;
}