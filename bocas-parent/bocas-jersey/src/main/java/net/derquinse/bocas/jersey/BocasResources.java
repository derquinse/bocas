/*
 * Copyright (C) the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.derquinse.bocas.jersey;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.annotation.Nullable;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.StreamingOutput;

import net.derquinse.bocas.BocasException;
import net.derquinse.bocas.BocasValue;
import net.derquinse.common.base.ByteString;
import net.derquinse.common.base.NotInstantiable;
import net.derquinse.common.util.zip.MaybeCompressed;

import com.google.common.base.CharMatcher;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.ByteStreams;

/**
 * Resource constants for Bocas RESTful API.
 * @author Andres Rodriguez.
 */
public final class BocasResources extends NotInstantiable {
	private BocasResources() {
	}

	/** Key response splitter. */
	private static final Splitter KEY_SPLITTER = Splitter.on(CharMatcher.WHITESPACE).trimResults().omitEmptyStrings();
	/** Key response splitter. */
	private static final Splitter ENTRY_SPLITTER = Splitter.on(':').trimResults().omitEmptyStrings();

	/** Key query parameter. */
	public static final String KEY = "k";

	/** Catalog resource. */
	public static final String CATALOG = "catalog";

	/** Zip resource. */
	public static final String ZIP = "zip";

	/** Zip and GZip resource. */
	public static final String ZIPGZIP = "zipgzip";

	/** Turns a non-null key into a string. */
	public static String checkKey(ByteString key) {
		return checkNotNull(key, "The object key must be provided").toHexString();
	}

	/** Turns a collection of keys into a string. */
	public static String iterable2String(Iterable<ByteString> keys) {
		checkNotNull(keys, "The object keys must be provided");
		StringBuilder b = new StringBuilder();
		for (ByteString key : keys) {
			b.append(checkKey(key)).append('\n');
		}
		return b.toString();
	}

	/**
	 * Turns a text/plain response into a list of keys.
	 * @throws BocasException if unable to perform the conversion.
	 */
	public static List<ByteString> response2List(@Nullable String response) {
		if (response == null) {
			return ImmutableList.of();
		}
		try {
			List<ByteString> list = Lists.newLinkedList();
			for (String k : BocasResources.KEY_SPLITTER.split(response)) {
				list.add(ByteString.fromHexString(k));
			}
			return list;
		} catch (RuntimeException e) {
			throw new BocasException(e);
		}
	}

	/** Turns a Bocas value into an streaming output. */
	public static StreamingOutput value2Output(final BocasValue value) {
		return new StreamingOutput() {
			@Override
			public void write(OutputStream output) throws IOException, WebApplicationException {
				ByteStreams.copy(value, output);
			}
		};
	}

	public static String zip2response(Map<String, ByteString> entries) {
		if (entries == null || entries.isEmpty()) {
			return "";
		}
		StringBuilder b = new StringBuilder();
		for (Entry<String, ByteString> entry : entries.entrySet()) {
			b.append(entry.getValue().toHexString()).append(':').append(entry.getKey()).append('\n');
		}
		return b.toString();
	}

	public static String czip2response(Map<String, MaybeCompressed<ByteString>> entries) {
		if (entries == null || entries.isEmpty()) {
			return "";
		}
		StringBuilder b = new StringBuilder();
		for (Entry<String, MaybeCompressed<ByteString>> entry : entries.entrySet()) {
			b.append(entry.getValue().getPayload().toHexString()).append(':')
					.append(entry.getValue().isCompressed() ? "1" : "0").append(':').append(entry.getKey()).append('\n');
		}
		return b.toString();
	}

	public static Map<String, ByteString> response2zip(String response) {
		if (response == null) {
			return ImmutableMap.of();
		}
		Map<String, ByteString> map = Maps.newHashMap();
		for (String line : KEY_SPLITTER.split(response)) {
			List<String> parts = Lists.newLinkedList(ENTRY_SPLITTER.split(line));
			if (parts.size() == 2) {
				map.put(parts.get(1), ByteString.fromHexString(parts.get(0)));
			}
		}
		return map;
	}

	public static Map<String, MaybeCompressed<ByteString>> response2czip(String response) {
		if (response == null) {
			return ImmutableMap.of();
		}
		Map<String, MaybeCompressed<ByteString>> map = Maps.newHashMap();
		for (String line : KEY_SPLITTER.split(response)) {
			List<String> parts = Lists.newLinkedList(ENTRY_SPLITTER.split(line));
			if (parts.size() == 3) {
				ByteString key = ByteString.fromHexString(parts.get(0));
				boolean compressed = "1".equals(parts.get(1));
				map.put(parts.get(2), MaybeCompressed.of(compressed, key));
			}
		}
		return map;
	}

}
