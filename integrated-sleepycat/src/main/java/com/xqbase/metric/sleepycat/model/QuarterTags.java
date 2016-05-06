package com.xqbase.metric.sleepycat.model;

import java.util.ArrayList;
import java.util.HashMap;

import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;
import com.sleepycat.persist.model.Relationship;
import com.sleepycat.persist.model.SecondaryKey;

@Entity
public class QuarterTags {
	@PrimaryKey(sequence="id")
	public long id;
	@SecondaryKey(relate=Relationship.MANY_TO_ONE)
	public String name;
	@SecondaryKey(relate=Relationship.MANY_TO_ONE)
	public int time;
	public HashMap<String, ArrayList<TagValue>> tags;
}