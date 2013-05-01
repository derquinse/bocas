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

import static com.google.common.base.Preconditions.checkNotNull;
import static net.derquinse.bocas.InternalUtils.convertCacheException;

import java.util.concurrent.TimeUnit;

import net.derquinse.common.base.ByteString;

import com.google.common.annotations.Beta;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableMap;
import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.google.common.io.ByteSource;

/**
 * Abstraction for a Bocas hash function.
 */
@Beta
public abstract class BocasHashFunction {
	private static final BocasHashFunction SHA256 = new SHA256();

	/** Available functions by name. */
	private static final ImmutableMap<String, BocasHashFunction> FUNCTIONS = ImmutableMap.of(SHA256.name(), SHA256);

	/** Returns the SHA-256 function. */
	public static BocasHashFunction sha256() {
		return SHA256;
	}

	/** Returns the available functions. */
	public static ImmutableMap<String, BocasHashFunction> getFunctions() {
		return FUNCTIONS;
	}

	/**
	 * Returns a hash function by name.
	 * @param name Function name.
	 * @return The requested function.
	 * @throws IllegalArgumentException if the requested function is not available.
	 */
	public static BocasHashFunction get(String name) {
		checkNotNull(name, "The hash function name must be provided");
		BocasHashFunction f = FUNCTIONS.get(name);
		if (f == null) {
			throw new IllegalArgumentException(String.format("Unknown hash function %s", name));
		}
		return f;
	}

	/** Hashing cache. */
	private final LoadingCache<ByteSource, ByteString> cache;

	/** Constructor. */
	private BocasHashFunction(final HashFunction f) {
		checkNotNull(f);
		this.cache = CacheBuilder.newBuilder().expireAfterAccess(1, TimeUnit.MINUTES).weakKeys()
				.build(new CacheLoader<ByteSource, ByteString>() {
					public ByteString load(ByteSource key) throws Exception {
						HashCode h = key.hash(f);
						return ByteString.copyFrom(h);
					}
				});
	}

	/** Returns the function name. */
	public abstract String name();

	/** Hashes a value. */
	public final ByteString hash(ByteSource value) {
		checkNotNull(value, "The value to hash must be provided");
		try {
			return cache.get(value);
		} catch (Throwable t) {
			throw convertCacheException(t);
		}
	}

	/** SHA-256 function. */
	private static final class SHA256 extends BocasHashFunction {
		SHA256() {
			super(Hashing.sha256());
		}

		@Override
		public String name() {
			return "SHA-256";
		}
	}

}
