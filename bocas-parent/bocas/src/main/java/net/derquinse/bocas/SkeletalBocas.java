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
package net.derquinse.bocas;

import java.util.List;
import java.util.Map;

import net.derquinse.common.base.ByteString;

import com.google.common.annotations.Beta;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;

/**
 * Base class for bucket implementations.
 * @author Andres Rodriguez.
 */
@Beta
public abstract class SkeletalBocas implements Bocas {
	/** Constructor. */
	protected SkeletalBocas() {
	}

	/** Turns a bocas value into a loaded value of the correct type. */
	protected abstract LoadedBocasValue load(BocasValue value);

	/*
	 * (non-Javadoc)
	 * @see net.derquinse.bocas.Bocas#put(net.derquinse.bocas.BocasValue)
	 */
	@Override
	public final ByteString put(BocasValue value) {
		LoadedBocasValue loaded = load(value);
		ByteString key = loaded.key();
		put(key, loaded);
		return key;
	}

	/** Puts an entry into the bucket. */
	protected abstract void put(ByteString key, LoadedBocasValue value);

	/*
	 * (non-Javadoc)
	 * @see net.derquinse.bocas.Bocas#putAll(java.lang.Iterable)
	 */
	@Override
	public final List<ByteString> putAll(Iterable<? extends BocasValue> values) {
		final ImmutableList.Builder<ByteString> keys = ImmutableList.builder();
		final Map<ByteString, LoadedBocasValue> entries = Maps.newHashMap();
		for (BocasValue value : values) {
			LoadedBocasValue loaded = load(value);
			ByteString key = loaded.key();
			keys.add(key);
			entries.put(key, loaded);
		}
		if (!entries.isEmpty()) {
			putAll(entries);
		}
		return keys.build();
	}

	/** Puts a collection of entries into the bucket. */
	protected abstract void putAll(Map<ByteString, LoadedBocasValue> entries);

}
