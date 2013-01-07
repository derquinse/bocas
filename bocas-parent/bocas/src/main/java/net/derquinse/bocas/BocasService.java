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

import com.google.common.annotations.Beta;

/**
 * Interface for a bocas repository. A Bocas repository consists on a set of buckets, identified by
 * a unique string. Whether the entries are shared among buckets or not is up to the implementation.
 * @author Andres Rodriguez.
 */
@Beta
public interface BocasService {
	/**
	 * Returns the bucket with the requested name.
	 * @throws NullPointerException if the argument is {@code null}.
	 * @throws IllegalArgumentException if there is no bucket with the provided name.
	 */
	Bocas getBucket(String name);
}
