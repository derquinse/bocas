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
package net.derquinse.bocas.gcs;

import java.io.File;

import net.derquinse.bocas.BocasHashFunction;
import net.derquinse.bocas.BocasService;
import net.derquinse.common.base.NotInstantiable;
import net.derquinse.common.io.MemoryByteSourceLoader;

/**
 * Factory class for Bocas repositories based on Google Cloud Storage.
 * @author Andres Rodriguez.
 */
public final class GCSBocas extends NotInstantiable {
	private GCSBocas() {
	}

	/**
	 * Creates a new service using service account authentication.
	 * @param email Account email address.
	 * @param p12 Private key file.
	 * @param function Hash function to use.
	 * @param loader Memory loader to use.
	 * @return The requested service.
	 */
	public static BocasService service(String email, File p12, BocasHashFunction function, MemoryByteSourceLoader loader) {
		return new GCSBocasService(email, p12, function, loader);
	}

}
