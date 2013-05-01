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

import static net.derquinse.bocas.InternalUtils.checkLoader;

import java.io.IOException;

import net.derquinse.common.io.MemoryByteSource;
import net.derquinse.common.io.MemoryByteSourceLoader;

import com.google.common.annotations.Beta;
import com.google.common.io.ByteSource;

/**
 * Abstract class for memory-based bocas buckets.
 * @author Andres Rodriguez.
 */
@Beta
abstract class AbstractMemoryBocas extends SkeletalBocas<MemoryByteSource> {
	/** Memory loader to use. */
	private final MemoryByteSourceLoader loader;

	/** Constructor. */
	AbstractMemoryBocas(BocasHashFunction function, MemoryByteSourceLoader loader) {
		super(function);
		this.loader = checkLoader(loader);
	}

	@Override
	protected final MemoryByteSource transform(ByteSource value) {
		try {
			return loader.load(value);
		} catch (IOException e) {
			throw new BocasException(e);
		}
	}
}
