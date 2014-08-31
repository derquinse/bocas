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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static net.derquinse.bocas.BocasPreconditions.checkHash;
import static net.derquinse.bocas.BocasPreconditions.checkLoader;

import java.io.File;

import net.derquinse.bocas.Bocas;
import net.derquinse.bocas.BocasException;
import net.derquinse.bocas.BocasHashFunction;
import net.derquinse.bocas.BocasService;
import net.derquinse.common.io.MemoryByteSourceLoader;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.storage.Storage;
import com.google.api.services.storage.StorageScopes;
import com.google.common.collect.ImmutableSet;

/**
 * Bocas repository based on Google Cloud Storage.
 * @author Andres Rodriguez.
 */
final class GCSBocasService implements BocasService {
	/** Global instance of the HTTP transport. */
	private static HttpTransport HTTP_TRANSPORT;

	/** Global instance of the JSON factory. */
	private static JsonFactory JSON_FACTORY;

	/** Credential to use for the service. */
	private final Storage storage;
	/** Hash function. */
	private final BocasHashFunction function;
	/** Memory loader. */
	private final MemoryByteSourceLoader loader;
	
	private static synchronized void init() {
		try {
			HTTP_TRANSPORT = GoogleNetHttpTransport.newTrustedTransport();
			JSON_FACTORY = JacksonFactory.getDefaultInstance();
		} catch(Exception e) {
			throw new BocasException(e);
		}
	}

	/**
	 * Constructor.
	 * @throws BocasException
	 */
	GCSBocasService(String email, File p12, BocasHashFunction function, MemoryByteSourceLoader loader) {
		checkNotNull(email);
		checkNotNull(p12);
		this.function = checkHash(function);
		this.loader = checkLoader(loader);
		checkArgument(p12.exists());
		init();
		try {
			// Build service account credential.
			GoogleCredential credential = new GoogleCredential.Builder().setTransport(HTTP_TRANSPORT)
					.setJsonFactory(JSON_FACTORY).setServiceAccountId(email).setServiceAccountScopes(ImmutableSet.of(StorageScopes.DEVSTORAGE_READ_WRITE))
					.setServiceAccountPrivateKeyFromP12File(p12).build();
			this.storage = new Storage.Builder(HTTP_TRANSPORT, JSON_FACTORY, credential).setApplicationName("GCSBocasTest").build();
		} catch (Exception e) {
			throw new BocasException(e);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see net.derquinse.bocas.BocasService#getBucket(java.lang.String)
	 */
	@Override
	public Bocas getBucket(String name) {
		return new GCSBucket(storage, name, function, loader);
	}

}
