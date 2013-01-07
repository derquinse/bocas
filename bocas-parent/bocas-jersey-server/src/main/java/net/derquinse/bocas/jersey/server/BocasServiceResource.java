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
package net.derquinse.bocas.jersey.server;

import static com.google.common.base.Preconditions.checkNotNull;

import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.WebApplicationException;

import net.derquinse.bocas.Bocas;
import net.derquinse.bocas.BocasService;

import com.sun.jersey.api.NotFoundException;

/**
 * Bocas repository JAX-RS resource. This class can be used as a sub-resource or subclassed to
 * become a root resource.
 * @author Andres Rodriguez.
 */
public class BocasServiceResource {
	/** Repository. */
	private final BocasService repository;

	private static WebApplicationException notFound() {
		throw new NotFoundException();
	}

	/** Constructor. */
	public BocasServiceResource(BocasService repository) {
		this.repository = checkNotNull(repository);
	}

	/** @see BocasService#getBucket(String) */
	@Path("{bucketName}")
	public BocasResource getBucket(@PathParam("bucketName") String bucketName) {
		final Bocas bucket;
		try {
			bucket = repository.getBucket(bucketName);
		} catch (IllegalArgumentException e) {
			throw notFound();
		}
		return new BocasResource(bucket);
	}
}
