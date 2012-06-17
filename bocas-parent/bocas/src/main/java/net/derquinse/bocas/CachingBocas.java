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
import com.google.common.cache.CacheStats;

/**
 * Interface for a caching BOCAS repository. Even though the statistics are based on Guava's
 * {@link CacheStats} objects implementations are not required to use Guava's caches.
 * @author Andres Rodriguez.
 */
@Beta
public interface CachingBocas extends Bocas {
	/**
	 * Returns the cache statistics.
	 * @return A current snapshot of the recorded statistics or an empty object if no statistics are
	 *         being recorded.
	 */
	CacheStats stats();
}
