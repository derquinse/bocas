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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.annotation.PreDestroy;
import javax.annotation.concurrent.GuardedBy;

import net.derquinse.bocas.Bocas;
import net.derquinse.bocas.BocasEntry;
import net.derquinse.bocas.BocasException;
import net.derquinse.bocas.BocasValue;
import net.derquinse.common.base.ByteString;
import net.derquinse.common.base.NotInstantiable;

import com.beust.jcommander.internal.Maps;
import com.google.common.annotations.Beta;
import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.io.ByteStreams;
import com.google.common.io.InputSupplier;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Transaction;

/**
 * Factory class for Bocas repositories based on Berkeley DB Java Edition.
 * @author Andres Rodriguez.
 */
public final class JEBocas extends NotInstantiable {
	private JEBocas() {
	}

	/**
	 * Creates a new repository based on a basic environment in the provided directory.
	 * @param directory Database environment directory. It must be an existing directory.
	 * @return The created service.
	 * @throws IllegalArgumentException if the argument is not an existing directory.
	 * @throws BocasException if unable to create the environment or database.
	 */
	public static Bocas basic(String directory) {
		checkNotNull(directory, "The environment directory must be provided");
		File d = new File(directory);
		checkArgument(d.exists(), "The directory [%] does not exist");
		checkArgument(d.isDirectory(), "The provided file [%] is not a directory");
		EnvironmentConfig ec = new EnvironmentConfig();
		ec.setAllowCreate(true);
		ec.setReadOnly(false);
		ec.setTransactional(true);
		try {
			Environment e = new Environment(d, ec);
			return new DefaultJEBocas(e);
		} catch (DatabaseException e) {
			throw new BocasException(e);
		}
	}
}
