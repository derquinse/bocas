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

import net.derquinse.common.base.NotInstantiable;

import com.google.common.annotations.Beta;

/**
 * Factory class for Bocas services and repositories.
 */
@Beta
public final class BocasServices extends NotInstantiable {
	private BocasServices() {
	}

	/** Creates a new heap-based repository. */
	public static Bocas memory() {
		return new MemoryBocas();
	}

	/** Creates a new memory-based repository based on direct buffers. */
	public static Bocas direct() {
		return new DirectMemoryBocas();
	}

	/** Creates a new caching repository builder. */
	public static CachingBocasBuilder cache() {
		return new CachingBocasBuilder();
	}

	/**
	 * Creates a new bocas repository that fetches entries missing in the primary repository from the
	 * provided seed. Writes are not propagated to the seed.
	 */
	public static Bocas seeded(Bocas primary, Bocas seed) {
		return new SeededBocas(primary, seed);
	}

}
