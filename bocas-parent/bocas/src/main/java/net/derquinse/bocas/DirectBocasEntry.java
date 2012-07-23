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
 * A Bocas repository value loaded into a direct byte buffer.
 * @author Andres Rodriguez.
 */
@Beta
public final class DirectBocasEntry extends BocasEntry {
	/** Payload. */
	private final DirectBocasValue value;

	DirectBocasEntry(DirectBocasValue value) {
		super(value);
		this.value = value;
	}

	/*
	 * (non-Javadoc)
	 * @see net.derquinse.bocas.BocasEntry#getValue()
	 */
	@Override
	public DirectBocasValue getValue() {
		return value;
	}

	/*
	 * (non-Javadoc)
	 * @see net.derquinse.bocas.BocasEntry#direct()
	 */
	@Override
	public DirectBocasEntry direct() {
		return this;
	}
}
