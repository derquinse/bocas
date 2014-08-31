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

import static com.google.common.base.Preconditions.checkNotNull;
import static net.derquinse.bocas.BocasPreconditions.checkLoader;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import net.derquinse.bocas.BocasException;
import net.derquinse.bocas.BocasHashFunction;
import net.derquinse.bocas.SimpleSkeletalBocas;
import net.derquinse.common.base.ByteString;
import net.derquinse.common.io.MemoryByteSource;
import net.derquinse.common.io.MemoryByteSourceLoader;
import net.derquinse.common.io.MemoryOutputStream;

import com.google.api.client.http.HttpResponseException;
import com.google.api.client.http.InputStreamContent;
import com.google.api.services.storage.Storage;
import com.google.api.services.storage.model.StorageObject;
import com.google.common.annotations.Beta;
import com.google.common.base.Optional;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.ByteSource;

/**
 * A bocas bucket based on Google Cloud Storage.
 * @author Andres Rodriguez.
 */
@Beta
final class GCSBucket extends SimpleSkeletalBocas {
	/** Storage service. */
	private final Storage storage;
	/** Bucket name. */
	private final String bucket;
	/** Memory loader to use. */
	private final MemoryByteSourceLoader loader;

	private static void checkKey(ByteString key) {
		checkNotNull(key, "The object key must be provided");
	}

	private static Set<ByteString> checkKeys(Iterable<ByteString> keys) {
		checkNotNull(keys, "The object keys must be provided");
		final Set<ByteString> set = Sets.newHashSet();
		for (ByteString key : keys) {
			checkNotNull(key, "Null keys not allowed");
			set.add(key);
		}
		return set;
	}

	/** Constructor. */
	GCSBucket(Storage storage, String bucket, BocasHashFunction function, MemoryByteSourceLoader loader) {
		super(function);
		this.storage = checkNotNull(storage);
		this.bucket = checkNotNull(bucket);
		this.loader = checkLoader(loader);
	}

	private Storage.Objects.Get getObjectRequest(final ByteString key) {
		checkKey(key);
		try {
			return storage.objects().get(bucket, key.toHexString());
		} catch (IOException e) {
			throw new BocasException(e);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see net.derquinse.bocas.Bocas#close()
	 */
	@Override
	public void close() {
		// Nothing to do.
	}

	/*
	 * (non-Javadoc)
	 * @see net.derquinse.bocas.Bocas#contains(net.derquinse.common.base.ByteString)
	 */
	@Override
	public boolean contains(final ByteString key) {
		checkKey(key);
		final Storage.Objects.Get request = getObjectRequest(key);
		try {
			StorageObject obj = request.execute();
			return obj != null;
		} catch (HttpResponseException e) {
			if (e.getStatusCode() == 404) {
				return false;
			}
			throw new BocasException(e);
		} catch (IOException e) {
			throw new BocasException(e);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see net.derquinse.bocas.Bocas#contained(java.lang.Iterable)
	 */
	@Override
	public Set<ByteString> contained(Iterable<ByteString> keys) {
		// TODO: check batch requests
		Set<ByteString> input = checkKeys(keys);
		Set<ByteString> set = Sets.newHashSet();
		for (ByteString key : input) {
			if (contains(key)) {
				set.add(key);
			}
		}
		return set;
	}

	/*
	 * (non-Javadoc)
	 * @see net.derquinse.bocas.Bocas#get(net.derquinse.common.base.ByteString)
	 */
	@Override
	public Optional<ByteSource> get(ByteString key) {
		checkKey(key);
		final Storage.Objects.Get request = getObjectRequest(key);
		final MemoryOutputStream os = loader.openStream();
		try {
			// TODO: check direct downloads.
			request.executeMediaAndDownloadTo(os);
			return Optional.of((ByteSource) os.toByteSource());
		} catch (HttpResponseException e) {
			if (e.getStatusCode() == 404) {
				return Optional.absent();
			}
			throw new BocasException(e);
		} catch (IOException e) {
			throw new BocasException(e);
		} finally {
			os.close();
		}
	}

	/*
	 * (non-Javadoc)
	 * @see net.derquinse.bocas.Bocas#get(java.lang.Iterable)
	 */
	@Override
	public Map<ByteString, ByteSource> get(final Iterable<ByteString> keys) {
		Set<ByteString> input = checkKeys(keys);
		Map<ByteString, ByteSource> map = Maps.newHashMap();
		for (ByteString key : input) {
			checkKey(key);
			if (!map.containsKey(key)) {
				Optional<ByteSource> v = get(key);
				if (v.isPresent()) {
					map.put(key, v.get());
				}
			}
		}
		return map;
	}

	@Override
	protected void put(final ByteString key, final ByteSource value) {
		boolean ok = false;
		try {
			final InputStream is = value.openStream();
			try {
				InputStreamContent content = new InputStreamContent("application/octet-stream", is);
				if (value instanceof MemoryByteSource) {
					content.setLength(value.size());
				}
				Storage.Objects.Insert request = storage.objects().insert(bucket, null, content);
				request.setName(key.toHexString());
				// TODO: check direct upload
				request.execute();
				ok = true;
			} catch (IOException e) {
				throw new BocasException(e);
			} finally {
				is.close();
			}
		} catch (IOException e) {
			// ok is true if the exception is thrown during clean up.
			if (!ok) {
				throw new BocasException(e);
			}
		}
	}

	/*
	 * (non-Javadoc)
	 * @see net.derquinse.bocas.SkeletalBocas#putAll(java.util.Map)
	 */
	@Override
	protected void putAll(final Map<ByteString, ByteSource> entries) {
		for (Entry<ByteString, ByteSource> entry : entries.entrySet()) {
			put(entry.getKey(), entry.getValue());
		}
	}

}
