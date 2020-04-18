package com.xqbase.metric.util;

import java.io.IOException;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.reflect.ReflectDatumWriter;

import com.fasterxml.jackson.core.type.TypeReference;
import com.xqbase.metric.common.MetricValue;
import com.xqbase.util.ByteArrayQueue;
import com.xqbase.util.Log;

public class Codecs {
	private static ReflectDatumWriter<Map<String, String>> tagsWriter;
	private static ReflectDatumReader<Map<String, String>> tagsReader;
	private static ReflectDatumWriter<Map<String, Map<String, MetricValue>>> tagsWriterEx;
	private static ReflectDatumReader<Map<String, Map<String, MetricValue>>> tagsReaderEx;

	static {
		Schema tagsSchema = ReflectData.get().getSchema(new
				TypeReference<Map<String, String>>() {/**/}.getType());
		Schema tagsSchemaEx = ReflectData.get().getSchema(new
				TypeReference<Map<String, Map<String, MetricValue>>>() {/**/}.getType());
		tagsWriter = new ReflectDatumWriter<>(tagsSchema);
		tagsReader = new ReflectDatumReader<>(tagsSchema);
		tagsWriterEx = new ReflectDatumWriter<>(tagsSchemaEx);
		tagsReaderEx = new ReflectDatumReader<>(tagsSchemaEx);
	}

	private ByteArrayQueue baq = new ByteArrayQueue();
	private BinaryEncoder encoder = EncoderFactory.get().
			binaryEncoder(baq.getOutputStream(), null);
	private BinaryDecoder decoder = DecoderFactory.get().
			binaryDecoder(baq.getInputStream(), null);

	private static ThreadLocal<Codecs> codecs_ = ThreadLocal.withInitial(Codecs::new);

	private static <T> byte[] encode(ReflectDatumWriter<T> writer, T t) {
		Codecs codecs = codecs_.get();
		try {
			writer.write(t, codecs.encoder);
			codecs.encoder.flush();
			return codecs.baq.getBytes();
		} catch (IOException e) {
			Log.w(e.getMessage());
			return null;
		} finally {
			codecs.baq.clear();
		}
	}

	public static byte[] encode(Map<String, String> tags) {
		return encode(tagsWriter, tags);
	}

	public static byte[] encodeEx(Map<String, Map<String, MetricValue>> tags) {
		return encode(tagsWriterEx, tags);
	}

	private static <T> T decode(ReflectDatumReader<T> reader, byte[] data) {
		Codecs codecs = codecs_.get();
		codecs.baq.add(data);
		try {
			return reader.read(null, codecs.decoder);
		} catch (IOException e) {
			Log.w(e.getMessage());
			return null;
		} finally {
			codecs.baq.clear();
		}
	}

	public static Map<String, String> decode(byte[] data) {
		return decode(tagsReader, data);
	}

	public static Map<String, Map<String, MetricValue>> decodeEx(byte[] data) {
		return decode(tagsReaderEx, data);
	}
}