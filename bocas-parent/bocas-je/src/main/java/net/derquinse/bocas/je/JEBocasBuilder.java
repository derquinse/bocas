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
package net.derquinse.bocas.je;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static net.derquinse.bocas.BocasPreconditions.checkHash;

import java.io.File;

import net.derquinse.bocas.Bocas;
import net.derquinse.bocas.BocasException;
import net.derquinse.bocas.BocasHashFunction;

import com.google.common.base.Objects;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;

/**
 * Builder class for Bocas buckets based on Berkeley DB Java Edition.
 * @author Andres Rodriguez.
 */
public final class JEBocasBuilder {
	private static final long ONE_MB = 1024L * 1024L;

	/** Log file size in bytes. */
	private Long fileSize;
	/** Cache size in bytes. */
	private Long cacheSize;
	/** Whether the shared cache is used. */
	private Boolean sharedCache;
	/** Whether the database is read only. */
	private Boolean readOnly;
	/** Whether to use direct memory. */
	private boolean direct = false;
	/** Hash function. */
	private BocasHashFunction function = BocasHashFunction.sha256();
	/** Whether the direct memory flag has been set. */
	private boolean functionSet = false;

	JEBocasBuilder() {
	}

	/**
	 * Sets the log file size.
	 * @param size The log file size in bytes.
	 * @return This builder.
	 * @throws IllegalArgumentException if the argument is <= 0.
	 * @throws IllegalStateException if the file size has already been set.
	 */
	public JEBocasBuilder setFileSize(long size) {
		checkState(this.fileSize == null, "The log file size has already been set");
		checkArgument(size > 0, "The log file size must be > 0");
		this.fileSize = size;
		return this;
	}

	/**
	 * Sets the log file size.
	 * @param size The log file size in MBytes.
	 * @return This builder.
	 * @throws IllegalArgumentException if the argument is <= 0.
	 * @throws IllegalStateException if the file size has already been set.
	 */
	public JEBocasBuilder setFileSizeMB(int size) {
		return setFileSize(ONE_MB * size);
	}

	/**
	 * Sets the cache size.
	 * @param size The cache size in bytes.
	 * @return This builder.
	 * @throws IllegalArgumentException if the argument is <= 0.
	 * @throws IllegalStateException if the cache size has already been set.
	 */
	public JEBocasBuilder setCacheSize(long size) {
		checkState(this.cacheSize == null, "The cache file size has already been set");
		checkArgument(size > 0, "The cache file size must be > 0");
		this.cacheSize = size;
		return this;
	}

	/**
	 * Sets the cache size.
	 * @param size The cache size in MBytes.
	 * @return This builder.
	 * @throws IllegalArgumentException if the argument is <= 0.
	 * @throws IllegalStateException if the cache size has already been set.
	 */
	public JEBocasBuilder setCacheSizeMB(long size) {
		return setCacheSize(ONE_MB * size);
	}

	/**
	 * Specifies that the shared cache should be used.
	 * @return This builder.
	 * @throws IllegalStateException if the flag has already been set.
	 */
	public JEBocasBuilder shared() {
		checkState(this.sharedCache == null, "The shared cache flag has already been set");
		this.sharedCache = true;
		return this;
	}

	/**
	 * Specifies that the database used should be read only.
	 * @return This builder.
	 * @throws IllegalStateException if the flag has already been set.
	 */
	public JEBocasBuilder readOnly() {
		checkState(this.readOnly == null, "The read only cache flag has already been set");
		this.readOnly = true;
		return this;
	}

	/**
	 * Specifies that direct memory should be used.
	 * @return This builder.
	 * @throws IllegalStateException if the flag has already been set.
	 */
	public JEBocasBuilder direct() {
		checkState(!direct, "The direct memory flag has already been set");
		this.direct = true;
		return this;
	}

	/**
	 * Sets the hash function to use.
	 * @param function The hash function to use.
	 * @return This builder.
	 * @throws IllegalStateException if the hash function has already been set.
	 */
	public JEBocasBuilder hashFunction(BocasHashFunction function) {
		checkState(!functionSet, "The direct memory flag has already been set");
		checkHash(function);
		this.function = function;
		this.functionSet = true;
		return this;
	}

	/**
	 * Builds a new bucket.
	 * @throws IllegalArgumentException if the argument is not an existing directory.
	 * @throws BocasException if unable to create the environment or database.
	 */
	public Bocas build(String directory) {
		checkNotNull(directory, "The environment directory must be provided");
		File d = new File(directory);
		checkArgument(d.exists(), "The directory [%s] does not exist");
		checkArgument(d.isDirectory(), "The provided file [%s] is not a directory");
		final boolean readOnly = Objects.firstNonNull(this.readOnly, Boolean.FALSE);
		EnvironmentConfig ec = new EnvironmentConfig();
		ec.setAllowCreate(true);
		ec.setReadOnly(false);
		ec.setTransactional(true);
		if (fileSize != null) {
			ec.setConfigParam(EnvironmentConfig.LOG_FILE_MAX, fileSize.toString());
		}
		if (cacheSize != null) {
			ec.setCacheSize(cacheSize);
		}
		ec.setSharedCache(Objects.firstNonNull(sharedCache, Boolean.FALSE));
		try {
			Environment e = new Environment(d, ec);
			return new DefaultJEBocas(function, e, direct, readOnly);
		} catch (DatabaseException e) {
			throw new BocasException(e);
		}
	}
}
