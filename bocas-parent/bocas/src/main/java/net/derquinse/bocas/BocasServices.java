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
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;

/**
 * Factory class for bocas repositories and buckets.
 */
@Beta
public final class BocasServices extends NotInstantiable {
	private BocasServices() {
	}

	/** Creates a new heap-based bucket. */
	public static Bocas memoryBucket() {
		return new MemoryBocas();
	}

	/** Creates a new memory-based bucket based on direct buffers. */
	public static Bocas directBucket() {
		return new DirectMemoryBocas();
	}

	/** Creates a new caching repository builder. */
	public static GuavaCachingBocasBuilder cache() {
		return new GuavaCachingBocasBuilder();
	}

	/**
	 * Decorates an existing service. Bucket closing (both source and decorated) is the caller's
	 * responsibility.
	 * @param service Service to decorate.
	 * @param decorator Bucket decorator.
	 */
	public static BocasService decorate(BocasService service, BocasDecorator decorator) {
		return new BocasServiceDecorator(service, decorator);
	}

	/**
	 * Creates a new bocas bucket that fetches entries missing in the primary bucket from the provided
	 * seed. Writes are not propagated to the seed. Closing is a no-op.
	 */
	public static Bocas seeded(Bocas primary, Bocas seed) {
		return new SeededBocas(primary, seed);
	}

	/**
	 * Creates a new service that shares a single bucket among several names.
	 * @param bucket Bucket to share.
	 * @param validName Valid names.
	 */
	public static BocasService shared(Bocas bucket, Predicate<String> validName) {
		return new SharedBocasService(bucket, validName);
	}

	/**
	 * Creates a new service that shares a single bucket for every possible bucket name..
	 * @param bucket Bucket to share.
	 */
	public static BocasService shared(Bocas bucket) {
		return shared(bucket, Predicates.<String> alwaysTrue());
	}

	/**
	 * Creates a new bocas bucket that synchronously replicates writes to the primary bucket into a
	 * replica. Closing both buckets is the responsibility of the caller.
	 * @param primary Primary bucket.
	 * @param replica Replica bucket.
	 * @param check Whether to check for existing entries before writing.
	 */
	public static Bocas syncReplica(Bocas primary, Bocas replica, boolean check) {
		return new SyncReplicatedBocas(primary, replica, check);
	}

	/**
	 * Creates a new bocas service that synchronously replicates writes to the primary service buckets
	 * into the replica buckets. Bucket closing is the responsibility of the caller.
	 * @param primary Primary service.
	 * @param replica Replica service.
	 * @param check Whether to check for existing entries before writing.
	 */
	public static BocasService syncReplica(BocasService primary, BocasService replica, boolean check) {
		return decorate(primary, new SyncReplicatedBocasDecorator(replica, check));
	}

	/**
	 * Creates a new builder for a cache shared among multiple services.
	 */
	public static MultiServiceGuavaCacheBuilder multiServiceCache() {
		return new MultiServiceGuavaCacheBuilder();
	}

}
