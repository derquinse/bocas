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
package net.derquinse.bocas.jdbc;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static net.derquinse.bocas.BocasPreconditions.checkHash;

import javax.sql.DataSource;

import net.derquinse.bocas.Bocas;
import net.derquinse.bocas.BocasHashFunction;
import net.derquinse.common.io.MemoryByteSourceLoader;

/**
 * Builder class for Bocas buckets based on JDBC.
 * @author Andres Rodriguez.
 */
public final class JDBCBocasBuilder {
	/** Hash function. */
	private BocasHashFunction function = BocasHashFunction.sha256();
	/** Whether the hash function flag has been set. */
	private boolean functionSet = false;
	/** Data source dialect. */
	private JDBCBocasDialect dialect = JDBCBocasDialect.H2;
	/** Whether the dialect has been set. */
	private boolean dialectSet = false;
	/** Loader to use for values. */
	private MemoryByteSourceLoader loader = MemoryByteSourceLoader.get();
	/** Whether the loader has been set. */
	private boolean loaderSet = false;

	JDBCBocasBuilder() {
	}

	/**
	 * Sets the hash function to use.
	 * @param function The hash function to use.
	 * @return This builder.
	 * @throws IllegalStateException if the hash function has already been set.
	 */
	public JDBCBocasBuilder hashFunction(BocasHashFunction function) {
		checkState(!functionSet, "The direct memory flag has already been set");
		checkHash(function);
		this.function = function;
		this.functionSet = true;
		return this;
	}

	/**
	 * Sets the dialect to use.
	 * @param dialect The dialect to use.
	 * @return This builder.
	 * @throws IllegalStateException if the dialect has already been set.
	 */
	public JDBCBocasBuilder dialect(JDBCBocasDialect dialect) {
		checkState(!dialectSet, "The dialect has already been set");
		this.dialect = checkNotNull(dialect, "The data source dialect must be provided");
		this.dialectSet = true;
		return this;
	}

	/**
	 * Sets the value loader to use.
	 * @param loader The value loader to use.
	 * @return This builder.
	 * @throws IllegalStateException if the dialect has already been set.
	 */
	public JDBCBocasBuilder loader(MemoryByteSourceLoader loader) {
		checkState(!loaderSet, "The value loader has already been set");
		this.loader = checkNotNull(loader, "The value loader must be provided");
		this.loaderSet = true;
		return this;
	}

	/**
	 * Builds a new bucket.
	 * @param dataSource Data source to use.
	 */
	public Bocas build(DataSource dataSource) {
		return new JDBCBocas(function, dataSource, dialect, loader);
	}
}
