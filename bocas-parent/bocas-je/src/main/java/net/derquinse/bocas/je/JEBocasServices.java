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
package net.derquinse.bocas.je;

import net.derquinse.bocas.Bocas;
import net.derquinse.bocas.BocasException;
import net.derquinse.common.base.NotInstantiable;

/**
 * Factory class for Bocas buckets based on Berkeley DB Java Edition.
 * @author Andres Rodriguez.
 */
public final class JEBocasServices extends NotInstantiable {
	private JEBocasServices() {
	}

	/** Creates a new bucket builder. */
	public static JEBocasBuilder newBuilder() {
		return new JEBocasBuilder();
	}

	/**
	 * Creates a new bucket based on a basic environment in the provided directory.
	 * @param directory Database environment directory. It must be an existing directory.
	 * @return The created service.
	 * @throws IllegalArgumentException if the argument is not an existing directory.
	 * @throws BocasException if unable to create the environment or database.
	 */
	public static Bocas basic(String directory) {
		return newBuilder().build(directory);
	}
}
