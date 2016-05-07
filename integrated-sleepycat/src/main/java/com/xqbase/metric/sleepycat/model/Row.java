package com.xqbase.metric.sleepycat.model;

import java.util.HashMap;

import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;
import com.sleepycat.persist.model.Relationship;
import com.sleepycat.persist.model.SecondaryKey;

@Entity
public class Row {
	@PrimaryKey(sequence="id")
	public long id;
	@SecondaryKey(relate=Relationship.MANY_TO_ONE)
	public int time;
	public long count;
	public double sum, max, min, sqr;
	public HashMap<String, String> tags;

	@Override
	public String toString() {
		return "Row [id=" + id + ", time=" + time + ", count=" + count +
				", sum=" + sum + ", max=" + max + ", min=" + min + ", sqr=" +
				sqr + ", tags=" + tags + "]";
	}
}