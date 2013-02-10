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

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import com.google.common.annotations.Beta;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

/**
 * Bocas service decorators.
 * @author Andres Rodriguez.
 */
@Beta
final class BocasServiceDecorator implements BocasService {
	/** Decorated service. */
	private final BocasService service;
	/** Bucket decorator. */
	private final BocasDecorator decorator;
	/** Decorated buckets. */
	private final Cache<String, Bocas> buckets;

	/** Constructor. */
	BocasServiceDecorator(BocasService service, BocasDecorator decorator) {
		this.service = checkNotNull(service, "The bocas service to decorate must be provided");
		this.decorator = checkNotNull(decorator, "The bocas decorator must be provided");
		this.buckets = CacheBuilder.newBuilder().build();
	}

	/*
	 * (non-Javadoc)
	 * @see net.derquinse.bocas.BocasService#getBucket(java.lang.String)
	 */
	public Bocas getBucket(final String name) {
		checkNotNull(name, "The bucket name must be provided");
		Callable<Bocas> loader = new Callable<Bocas>() {
			@Override
			public Bocas call() throws Exception {
				Bocas original = service.getBucket(name);
				return decorator.decorate(name, original);
			}
		};
		try {
			return buckets.get(name, loader);
		} catch (ExecutionException e) {
			Throwable t = e.getCause();
			if (t instanceof BocasException) {
				throw (BocasException) t;
			}
			throw new BocasException(t);
		}
	}
}
