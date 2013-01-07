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

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.annotation.PreDestroy;
import javax.annotation.concurrent.GuardedBy;

import net.derquinse.bocas.BocasException;
import net.derquinse.bocas.BocasValue;
import net.derquinse.bocas.LoadedBocasValue;
import net.derquinse.bocas.SkeletalBocas;
import net.derquinse.common.base.ByteString;

import com.google.common.annotations.Beta;
import com.google.common.base.Optional;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.ByteStreams;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Transaction;

/**
 * A Bocas repository based on a Berkeley DB Java Edition environment.
 * @author Andres Rodriguez.
 */
@Beta
final class DefaultJEBocas extends SkeletalBocas {
	/** Database name. */
	private static final String DB_NAME = "BocasDB";
	/** Database environment. */
	private final Environment environment;
	/** Database. */
	private final Database database;
	/** Database closing lock. */
	private final ReadWriteLock lock = new ReentrantReadWriteLock();
	/** Whether the database is still open. */
	@GuardedBy("lock")
	private boolean open = true;

	private static void checkKey(ByteString key) {
		checkNotNull(key, "The object key must be provided");
	}

	private static void checkKeys(Iterable<ByteString> key) {
		checkNotNull(key, "The object keys must be provided");
	}

	/** Constructor. */
	DefaultJEBocas(Environment e) {
		this.environment = checkNotNull(e, "The environment must be provided");
		DatabaseConfig dc = new DatabaseConfig();
		dc.setAllowCreate(true);
		dc.setTransactional(true);
		dc.setReadOnly(false);
		this.database = e.openDatabase(null, DB_NAME, dc);
	}

	/*
	 * (non-Javadoc)
	 * @see net.derquinse.bocas.SkeletalBocas#load(net.derquinse.bocas.BocasValue)
	 */
	@Override
	protected LoadedBocasValue load(BocasValue value) {
		return value.toLoaded();
	}

	/** Closes the database. */
	@PreDestroy
	public void close() {
		lock.writeLock().lock();
		try {
			if (!open) {
				return;
			}
			try {
				database.close();
			} catch (Exception e) {
				// TODO: log
			}
			try {
				environment.close();
			} catch (Exception e) {
				// TODO: log
			}
		} finally {
			lock.writeLock().unlock();
		}
	}

	private void ensureOpen() {
		checkState(open, "Database already closed");
	}

	/*
	 * (non-Javadoc)
	 * @see net.derquinse.bocas.Bocas#contains(net.derquinse.common.base.ByteString)
	 */
	@Override
	public boolean contains(final ByteString key) {
		checkKey(key);
		return new Tx<Boolean>() {
			@Override
			Boolean perform() throws IOException {
				return read(key).isPresent();
			}
		}.run().booleanValue();
	}

	/*
	 * (non-Javadoc)
	 * @see net.derquinse.bocas.Bocas#contained(java.lang.Iterable)
	 */
	@Override
	public Set<ByteString> contained(final Iterable<ByteString> keys) {
		checkKeys(keys);
		return new Tx<Set<ByteString>>() {
			@Override
			Set<ByteString> perform() throws IOException {
				Set<ByteString> set = Sets.newHashSet();
				for (ByteString key : keys) {
					checkKey(key);
					if (!set.contains(key) && read(key).isPresent()) {
						set.add(key);
					}
				}
				return set;
			}
		}.run();
	}

	/*
	 * (non-Javadoc)
	 * @see net.derquinse.bocas.Bocas#get(net.derquinse.common.base.ByteString)
	 */
	@Override
	public Optional<BocasValue> get(final ByteString key) {
		checkKey(key);
		return new Tx<Optional<BocasValue>>() {
			@Override
			Optional<BocasValue> perform() throws IOException {
				return read(key);
			}
		}.run();
	}

	/*
	 * (non-Javadoc)
	 * @see net.derquinse.bocas.Bocas#get(java.lang.Iterable)
	 */
	@Override
	public Map<ByteString, BocasValue> get(final Iterable<ByteString> keys) {
		checkKeys(keys);
		return new Tx<Map<ByteString, BocasValue>>() {
			@Override
			Map<ByteString, BocasValue> perform() throws IOException {
				Map<ByteString, BocasValue> map = Maps.newHashMap();
				for (ByteString key : keys) {
					checkKey(key);
					if (!map.containsKey(key)) {
						Optional<BocasValue> v = read(key);
						if (v.isPresent()) {
							map.put(key, v.get());
						}
					}
				}
				return map;
			}
		}.run();
	}

	/*
	 * (non-Javadoc)
	 * @see net.derquinse.bocas.SkeletalBocas#put(net.derquinse.common.base.ByteString,
	 * net.derquinse.bocas.LoadedBocasValue)
	 */
	@Override
	protected void put(final ByteString key, final LoadedBocasValue value) {
		new Put() {
			@Override
			void put() throws IOException {
				write(key, value);
			}
		}.run();
	}

	/*
	 * (non-Javadoc)
	 * @see net.derquinse.bocas.SkeletalBocas#putAll(java.util.Map)
	 */
	@Override
	protected void putAll(final Map<ByteString, LoadedBocasValue> entries) {
		new Put() {
			@Override
			void put() throws IOException {
				for (Entry<ByteString, LoadedBocasValue> entry : entries.entrySet()) {
					write(entry.getKey(), entry.getValue());
				}
			}
		}.run();
	}

	/** Bocas transaction. */
	private abstract class Tx<T> {
		private Transaction tx;

		final T run() {
			lock.readLock().lock();
			try {
				ensureOpen();
				return runTx();
			} catch (DatabaseException e) {
				throw new BocasException(e);
			} catch (IOException e) {
				throw new BocasException(e);
			} finally {
				lock.readLock().unlock();
			}
		}

		private T runTx() throws IOException {
			boolean ok = false;
			tx = environment.beginTransaction(null, null);
			try {
				T value = perform();
				ok = true;
				return value;
			} finally {
				if (ok) {
					tx.commit();
				} else {
					tx.abort();
				}
			}
		}

		private DatabaseEntry key(ByteString key) {
			return new DatabaseEntry(key.toByteArray());
		}

		/**
		 * Reads an entry from the database.
		 * @param key Key to read.
		 * @return The entry value if found.
		 */
		final Optional<BocasValue> read(ByteString key) {
			DatabaseEntry k = key(key);
			DatabaseEntry v = new DatabaseEntry();
			if (database.get(tx, k, v, null) == OperationStatus.SUCCESS) {
				BocasValue bv = BocasValue.of(v.getData());
				return Optional.of(bv);
			}
			return Optional.absent();
		}

		/**
		 * Writes an entry, if absent
		 * @param key Entry key.
		 * @param value Entry value.
		 */
		final void write(ByteString key, LoadedBocasValue value) throws IOException {
			DatabaseEntry k = key(key);
			DatabaseEntry v = new DatabaseEntry(ByteStreams.toByteArray(value));
			database.putNoOverwrite(tx, k, v);
		}

		abstract T perform() throws IOException;
	}

	/** Put transactions. */
	private abstract class Put extends Tx<Object> {
		@Override
		final ByteString perform() throws IOException {
			put();
			return null;
		}

		abstract void put() throws IOException;
	}

}
