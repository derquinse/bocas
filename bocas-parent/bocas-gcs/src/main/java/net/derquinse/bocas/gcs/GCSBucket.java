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
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import net.derquinse.bocas.BocasException;
import net.derquinse.bocas.BocasHashFunction;
import net.derquinse.bocas.SimpleSkeletalBocas;
import net.derquinse.common.base.ByteString;
import net.derquinse.common.io.MemoryByteSource;
import net.derquinse.common.io.MemoryByteSourceLoader;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpContent;
import com.google.api.client.http.HttpHeaders;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestFactory;
import com.google.api.client.http.HttpResponse;
import com.google.api.client.http.HttpResponseException;
import com.google.api.client.http.InputStreamContent;
import com.google.common.annotations.Beta;
import com.google.common.base.Optional;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.ByteSource;
import com.google.common.io.Closeables;
import com.google.common.io.Closer;
import com.google.common.primitives.Ints;

/**
 * A bocas bucket based on Google Cloud Storage.
 * @author Andres Rodriguez.
 */
@Beta
final class GCSBucket extends SimpleSkeletalBocas {
	/** Date formatter. */
	private static final DateTimeFormatter RFC1123_DATE_FORMAT = DateTimeFormat
			.forPattern("EEE, dd MMM yyyy HH:mm:ss 'GMT'").withLocale(Locale.US).withZone(DateTimeZone.UTC);

	/** Request factory to use. */
	private final HttpRequestFactory requestFactory;
	/** Bucket URI (must not end in a slash). */
	private final String bucketURI;
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
	GCSBucket(HttpRequestFactory requestFactory, String bucketURI, BocasHashFunction function,
			MemoryByteSourceLoader loader) {
		super(function);
		this.requestFactory = checkNotNull(requestFactory);
		this.bucketURI = checkNotNull(bucketURI);
		this.loader = checkLoader(loader);
	}

	private GenericUrl getObjectURI(ByteString key) {
		return new GenericUrl(bucketURI + '/' + key.toHexString());
	}

	private HttpRequest decorate(HttpRequest request, Integer size) {
		HttpHeaders headers = request.getHeaders();
		headers.set("Host", "storage.googleapis.com");
		headers.setDate(RFC1123_DATE_FORMAT.print(DateTime.now()));
		headers.set("x-goog-api-version", "2");
		if (size != null) {
			headers.setContentLength(size.longValue());
			headers.setContentType("application/octet-stream");
		}
		return request;
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
		try {
			HttpRequest request = decorate(requestFactory.buildHeadRequest(getObjectURI(key)), null);
			HttpResponse response = request.execute();
			try {
				return response.isSuccessStatusCode();
			} finally {
				response.disconnect();
			}
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
		try {
			HttpRequest request = decorate(requestFactory.buildGetRequest(getObjectURI(key)), null);
			HttpResponse response = request.execute();
			try {
				if (response.isSuccessStatusCode()) {
					InputStream is = response.getContent();
					if (is == null) {
						return Optional.absent();
					}
					Closer closer = Closer.create();
					try {
						ByteSource value = loader.load(closer.register(is));
						return Optional.of(value);
					} finally {
						Closeables.closeQuietly(is);
					}
				}
			} finally {
				response.disconnect();
			}
			return Optional.absent();
		} catch (HttpResponseException e) {
			if (e.getStatusCode() == 404) {
				return Optional.absent();
			}
			throw new BocasException(e);
		} catch (IOException e) {
			throw new BocasException(e);
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
		try {
			final MemoryByteSource mbs;
			if (value instanceof MemoryByteSource) {
				mbs = (MemoryByteSource) value;
			} else {
				mbs = loader.load(value);
			}
			HttpContent content = new InputStreamContent("application/octet-stream", mbs.openStream());
			HttpRequest request = decorate(requestFactory.buildPutRequest(getObjectURI(key), content),
					Ints.saturatedCast(mbs.size()));
			request.execute();
		} catch (IOException e) {
			throw new BocasException(e);
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
