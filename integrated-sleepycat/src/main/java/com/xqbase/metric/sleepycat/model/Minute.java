package com.xqbase.metric.sleepycat.model;

import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;
import com.sleepycat.persist.model.Relationship;
import com.sleepycat.persist.model.SecondaryKey;

@Entity
public class Minute {
	@PrimaryKey(sequence="id")
	public long id;
	@SecondaryKey(relate=Relationship.MANY_TO_ONE)
	public int minute;
}