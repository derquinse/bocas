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

import static net.derquinse.bocas.InternalUtils.checkTransformedValue;
import static net.derquinse.bocas.InternalUtils.checkValue;

import java.util.List;
import java.util.Map;

import net.derquinse.common.base.ByteString;

import com.google.common.annotations.Beta;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.io.ByteSource;

/**
 * Base class for bucket implementations.
 * @author Andres Rodriguez.
 * @param <T> Value type.
 */
@Beta
public abstract class SkeletalBocas<T extends ByteSource> implements Bocas {
	/** Hash function to use. */
	private final BocasHashFunction function;

	/** Constructor. */
	protected SkeletalBocas(BocasHashFunction function) {
		this.function = BocasPreconditions.checkHash(function);
	}

	/** Transforms a byte source before putting it. */
	protected abstract T transform(ByteSource value);

	/** Obtains the transformed value. */
	private T transformed(ByteSource value) {
		return checkTransformedValue(transform(checkValue(value)));
	}

	/*
	 * (non-Javadoc)
	 * @see net.derquinse.bocas.Bocas#getHashFunction()
	 */
	@Override
	public final BocasHashFunction getHashFunction() {
		return function;
	}

	/*
	 * (non-Javadoc)
	 * @see net.derquinse.bocas.Bocas#put(com.google.common.io.ByteSource)
	 */
	@Override
	public final ByteString put(ByteSource value) {
		T finalValue = transformed(value);
		ByteString key = function.hash(finalValue);
		put(key, finalValue);
		return key;
	}

	/** Puts an entry into the bucket. */
	protected abstract void put(ByteString key, T value);

	/*
	 * (non-Javadoc)
	 * @see net.derquinse.bocas.Bocas#putAll(java.lang.Iterable)
	 */
	@Override
	public final List<ByteString> putAll(Iterable<? extends ByteSource> values) {
		final ImmutableList.Builder<ByteString> keys = ImmutableList.builder();
		final Map<ByteString, T> entries = Maps.newHashMap();
		for (ByteSource value : values) {
			T finalValue = transformed(value);
			ByteString key = function.hash(finalValue);
			keys.add(key);
			entries.put(key, finalValue);
		}
		if (!entries.isEmpty()) {
			putAll(entries);
		}
		return keys.build();
	}

	/** Puts a collection of entries into the bucket. */
	protected abstract void putAll(Map<ByteString, T> entries);

}
