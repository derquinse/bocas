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

import java.net.URI;

import net.derquinse.bocas.BocasService;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;

/**
 * Factory for Bocas repository clients based on Jersey (JAX-RS).
 * @author Andres Rodriguez.
 */
public final class BocasClientFactory {
	/** Jersey client. */
	private final Client client;

	/** Creates a new factory. */
	public static BocasClientFactory create() {
		return new BocasClientFactory();
	}

	/** Constructor. */
	private BocasClientFactory() {
		ClientConfig config = new DefaultClientConfig();
		client = Client.create(config);
		client.setFollowRedirects(true);
	}

	/**
	 * Creates a new repository client.
	 * @param uri Service URI.
	 * @return The requested service client.
	 */
	public BocasService get(URI uri) {
		WebResource resource = client.resource(checkNotNull(uri, "The indexer service URI must be provided"));
		return new BocasServiceClient(resource);
	}

}
