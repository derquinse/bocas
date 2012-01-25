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

import java.util.List;

import javax.annotation.Nullable;

import net.derquinse.bocas.BocasException;
import net.derquinse.common.base.ByteString;
import net.derquinse.common.base.NotInstantiable;

import com.google.common.base.CharMatcher;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

/**
 * Resource constants for Bocas RESTful API.
 * @author Andres Rodriguez.
 */
public final class BocasResources extends NotInstantiable {
	private BocasResources() {
	}

	/** Key response splitter. */
	private static final Splitter KEY_SPLITTER = Splitter.on(CharMatcher.WHITESPACE).trimResults().omitEmptyStrings();

	/** Key query parameter. */
	public static final String KEY = "k";

	/** Catalog resource. */
	public static final String CATALOG = "catalog";

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

}
