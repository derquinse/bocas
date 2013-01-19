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

import java.io.File;

import net.derquinse.bocas.Bocas;
import net.derquinse.bocas.BocasException;
import net.derquinse.bocas.BocasService;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.http.HttpRequestFactory;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.gson.GsonFactory;

/**
 * Bocas repository based on Google Cloud Storage.
 * @author Andres Rodriguez.
 */
final class GCSBocasService implements BocasService {
	/** Base URL. */
	private static final String GCS_URL = "http://storage.googleapis.com/";

	/** Global configuration of Google Cloud Storage OAuth 2.0 scope. */
	private static final String GCS_SCOPE = "https://www.googleapis.com/auth/devstorage.read_write";

	/** Global instance of the HTTP transport. */
	private static final HttpTransport HTTP_TRANSPORT = new NetHttpTransport();

	/** Global instance of the JSON factory. */
	private static final JsonFactory JSON_FACTORY = new GsonFactory();

	/** Credential to use for the service. */
	private final HttpRequestFactory factory;

	/**
	 * Constructor.
	 * @throws BocasException
	 */
	GCSBocasService(String email, File p12) {
		checkNotNull(email);
		checkNotNull(p12);
		checkArgument(p12.exists());
		try {
			// Build service account credential.
			GoogleCredential credential = new GoogleCredential.Builder().setTransport(HTTP_TRANSPORT)
					.setJsonFactory(JSON_FACTORY).setServiceAccountId(email).setServiceAccountScopes(GCS_SCOPE)
					.setServiceAccountPrivateKeyFromP12File(p12).build();
			this.factory = HTTP_TRANSPORT.createRequestFactory(credential);
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
		return new GCSBucket(factory, GCS_URL + name);
	}

}