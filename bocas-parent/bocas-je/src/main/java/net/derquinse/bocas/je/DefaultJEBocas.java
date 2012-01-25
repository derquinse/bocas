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

import com.google.common.annotations.Beta;
import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.ByteStreams;
import com.google.common.io.InputSupplier;
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
final class DefaultJEBocas implements Bocas {
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

	private static void checkValue(Object value) {
		checkNotNull(value, "The object value must be provided");
	}

	private static void checkValues(Object values) {
		checkNotNull(values, "The object values must be provided");
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
	 * @see net.derquinse.bocas.Bocas#put(com.google.common.io.InputSupplier)
	 */
	@Override
	public ByteString put(final InputSupplier<? extends InputStream> object) {
		checkValue(object);
		return new Put() {
			@Override
			byte[] getData() throws IOException {
				return ByteStreams.toByteArray(object);
			}
		}.run();
	}

	/*
	 * (non-Javadoc)
	 * @see net.derquinse.bocas.Bocas#put(java.io.InputStream)
	 */
	@Override
	public ByteString put(final InputStream object) {
		checkValue(object);
		return new Put() {
			@Override
			byte[] getData() throws IOException {
				return ByteStreams.toByteArray(object);
			}
		}.run();
	}

	/*
	 * (non-Javadoc)
	 * @see net.derquinse.bocas.Bocas#putAll(java.util.List)
	 */
	@Override
	public List<ByteString> putSuppliers(final List<? extends InputSupplier<? extends InputStream>> objects) {
		checkValues(objects);
		return new MultiPut<InputSupplier<? extends InputStream>>() {
			@Override
			List<? extends InputSupplier<? extends InputStream>> getObjects() {
				return objects;
			}

			@Override
			byte[] getData(InputSupplier<? extends InputStream> object) throws IOException {
				return ByteStreams.toByteArray(object);
			}
		}.run();
	}

	/*
	 * (non-Javadoc)
	 * @see net.derquinse.bocas.Bocas#putStreams(java.util.List)
	 */
	@Override
	public List<ByteString> putStreams(final List<? extends InputStream> objects) {
		checkValues(objects);
		return new MultiPut<InputStream>() {
			@Override
			List<? extends InputStream> getObjects() {
				return objects;
			}

			@Override
			byte[] getData(InputStream object) throws IOException {
				return ByteStreams.toByteArray(object);
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
		 * @param data Data to write.
		 * @return The entry key.
		 */
		final ByteString write(byte[] data) {
			BocasEntry entry = BocasEntry.of(data);
			DatabaseEntry k = key(entry.getKey());
			DatabaseEntry v = new DatabaseEntry(data);
			database.putNoOverwrite(tx, k, v);
			return entry.getKey();
		}

		abstract T perform() throws IOException;
	}

	/** Put transaction. */
	private abstract class Put extends Tx<ByteString> {
		@Override
		final ByteString perform() throws IOException {
			return write(getData());
		}

		abstract byte[] getData() throws IOException;
	}

	/** Put multiple objects transaction. */
	private abstract class MultiPut<S> extends Tx<List<ByteString>> {
		@Override
		final List<ByteString> perform() throws IOException {
			List<byte[]> entries = Lists.newArrayListWithCapacity(getObjects().size());
			for (S object : getObjects()) {
				checkValue(object);
				entries.add(getData(object));
			}
			List<ByteString> keys = Lists.newArrayListWithCapacity(entries.size());
			for (byte[] entry : entries) {
				keys.add(write(entry));
			}
			return keys;
		}

		abstract List<? extends S> getObjects();

		abstract byte[] getData(S object) throws IOException;
	}

}
