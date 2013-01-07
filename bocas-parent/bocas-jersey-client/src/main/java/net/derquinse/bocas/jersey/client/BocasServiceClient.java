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
package net.derquinse.bocas.jersey.client;

import static com.google.common.base.Preconditions.checkNotNull;
import net.derquinse.bocas.Bocas;
import net.derquinse.bocas.BocasService;

import com.sun.jersey.api.client.WebResource;

/**
 * Bocas repository client based on Jersey (JAX-RS).
 * @author Andres Rodriguez.
 */
final class BocasServiceClient implements BocasService {
	/** Root resource. */
	private final WebResource resource;

	BocasServiceClient(WebResource resource) {
		this.resource = checkNotNull(resource, "The root resource must be provided");
	}

	@Override
	public Bocas getBucket(String name) {
		checkNotNull(name);
		return new BocasClient(resource.path(name)); // TODO ping bucket.
	}
}
