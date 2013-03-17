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

import com.google.common.cache.Weigher;

/**
 * Guava-based cache entry weigher.
 * @author Andres Rodriguez.
 */
enum EntryWeigher implements Weigher<Object, BocasValue> {
	INSTANCE;

	@Override
	public int weigh(Object key, BocasValue value) {
		Integer size = value.getSize();
		return size != null ? 16 + size.intValue() : 8192;
	}
}