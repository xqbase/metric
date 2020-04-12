package com.xqbase.metric.util;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoException;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.xqbase.util.ByteArrayQueue;
import com.xqbase.util.Bytes;
import com.xqbase.util.Log;

public class Codecs {
	private static ThreadLocal<Kryo> kryo = ThreadLocal.withInitial(Kryo::new);

	public static byte[] serialize(Object o) {
		ByteArrayQueue baq = new ByteArrayQueue();
		try (Output output = new Output(baq.getOutputStream())) {
			kryo.get().writeObject(output, o);
		}
		return Bytes.sub(baq.array(), baq.offset(), baq.length());
	}

	/** Be sure to do null pointer check on return value !!! */
	@SuppressWarnings("unchecked")
	public static <T> T deserialize(byte[] b, T t) {
		try (Input input = new Input(b)) {
			return (T) kryo.get().readObject(input, t.getClass());
		} catch (KryoException e) {
			Log.w(e.getMessage());
			return null;
		}
	}
}