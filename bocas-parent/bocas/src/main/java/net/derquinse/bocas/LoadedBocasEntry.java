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
 * A BOCAS repository value loaded into an in-memory byte array.
 * @author Andres Rodriguez.
 */
@Beta
public final class LoadedBocasEntry extends BocasEntry {
	/** Payload. */
	private final LoadedBocasValue value;

	LoadedBocasEntry(LoadedBocasValue value) {
		super(value);
		this.value = value;
	}

	/*
	 * (non-Javadoc)
	 * @see net.derquinse.bocas.BocasEntry#getValue()
	 */
	@Override
	public LoadedBocasValue getValue() {
		return value;
	}

	/*
	 * (non-Javadoc)
	 * @see net.derquinse.bocas.BocasEntry#load()
	 */
	@Override
	public LoadedBocasEntry load() {
		return this;
	}
}
