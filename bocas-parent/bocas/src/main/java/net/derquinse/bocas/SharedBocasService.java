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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.base.Predicate;

/**
 * A bocas repository that shares entry among a set of buckets.
 * @author Andres Rodriguez.
 */
final class SharedBocasService implements BocasService {
	/** Shared bucket. */
	private final Bocas bucket;
	/** Valid bucket names. */
	private final Predicate<String> validName;

	SharedBocasService(Bocas bucket, Predicate<String> validName) {
		this.bucket = checkNotNull(bucket);
		this.validName = checkNotNull(validName);
	}

	/*
	 * (non-Javadoc)
	 * @see net.derquinse.bocas.BocasService#getBucket(java.lang.String)
	 */
	public Bocas getBucket(String name) {
		checkArgument(validName.apply(checkNotNull(name)));
		return bucket;
	}
}
