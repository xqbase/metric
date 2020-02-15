package com.xqbase.metric.util;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.introspect.AnnotatedMember;
import com.fasterxml.jackson.databind.introspect.JacksonAnnotationIntrospector;
import com.fasterxml.jackson.databind.type.TypeFactory;
import com.xqbase.metric.common.MetricValue;
import com.xqbase.util.Log;

import de.undercouch.bson4jackson.BsonFactory;

public class Codecs {
	private static final BsonFactory bf = new BsonFactory();
	private static ObjectMapper tagsWriter = new ObjectMapper(bf);
	private static ObjectMapper tagsWriterEx = new ObjectMapper(bf);
	private static ObjectMapper tagsReader = new ObjectMapper(bf);
	private static JavaType type, typeEx;

	static {
		Set<String> ignoreMethods = new HashSet<>(Arrays.asList("getAvg", "getStd"));
		tagsWriterEx.setAnnotationIntrospector(new JacksonAnnotationIntrospector() {
			private static final long serialVersionUID = 1L;

			@Override
			public boolean hasIgnoreMarker(AnnotatedMember m) {
				return ignoreMethods.contains(m.getName());
			}
		});
		TypeFactory tf = TypeFactory.defaultInstance();
		type = tf.constructType(new TypeReference<Map<String, String>>() {/**/});
		typeEx = tf.constructType(new TypeReference<Map<String, Map<String, MetricValue>>>() {/**/});
	}

	public static byte[] encode(Map<String, String> tags) {
		try {
			return tagsWriter.writeValueAsBytes(tags);
		} catch (IOException e) {
			Log.w(e.getMessage());
			return null;
		}
	}

	public static byte[] encodeEx(Map<String, Map<String, MetricValue>> tags) {
		try {
			return tagsWriterEx.writeValueAsBytes(tags);
		} catch (IOException e) {
			Log.w(e.getMessage());
			return null;
		}
	}

	public static Map<String, String> decode(byte[] data) {
		try {
			return tagsReader.readValue(data, type);
		} catch (IOException e) {
			Log.w(e.getMessage());
			return null;
		}
	}

	public static Map<String, Map<String, MetricValue>> decodeEx(byte[] data) {
		try {
			return tagsReader.readValue(data, typeEx);
		} catch (IOException e) {
			Log.w(e.getMessage());
			return null;
		}
	}
}