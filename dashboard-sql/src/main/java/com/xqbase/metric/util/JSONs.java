package com.xqbase.metric.util;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.introspect.AnnotatedMember;
import com.fasterxml.jackson.databind.introspect.JacksonAnnotationIntrospector;
import com.fasterxml.jackson.databind.type.TypeFactory;
import com.xqbase.metric.common.MetricValue;
import com.xqbase.util.Log;

public class JSONs {
	private static ObjectMapper tagsWriter = new ObjectMapper();
	private static ObjectMapper tagsWriterEx = new ObjectMapper();
	private static ObjectMapper tagsReader = new ObjectMapper();
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

	public static String serialize(Map<String, String> tags) {
		try {
			return tagsWriter.writeValueAsString(tags);
		} catch (JsonProcessingException e) {
			Log.w(e.getMessage());
			return null;
		}
	}

	public static String serializeEx(Map<String, Map<String, MetricValue>> tags) {
		try {
			return tagsWriterEx.writeValueAsString(tags);
		} catch (JsonProcessingException e) {
			Log.w(e.getMessage());
			return null;
		}
	}

	public static Map<String, String> deserialize(String json) {
		try {
			return tagsReader.readValue(json, type);
		} catch (JsonProcessingException e) {
			Log.w(e.getMessage());
			return null;
		}
	}

	public static Map<String, Map<String, MetricValue>> deserializeEx(String json) {
		try {
			return tagsReader.readValue(json, typeEx);
		} catch (JsonProcessingException e) {
			Log.w(e.getMessage());
			return null;
		}
	}
}