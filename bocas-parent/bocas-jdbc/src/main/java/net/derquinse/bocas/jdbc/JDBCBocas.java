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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import javax.sql.DataSource;

import net.derquinse.bocas.BocasException;
import net.derquinse.bocas.BocasHashFunction;
import net.derquinse.bocas.SimpleSkeletalBocas;
import net.derquinse.common.base.ByteString;
import net.derquinse.common.io.MemoryByteSourceLoader;

import com.google.common.annotations.Beta;
import com.google.common.base.Optional;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.ByteSource;

/**
 * A Bocas repository based on a JDBC DataSource.
 * @author Andres Rodriguez.
 */
@Beta
final class JDBCBocas extends SimpleSkeletalBocas {
	/** Data source. */
	private final DataSource dataSource;
	/** Data source dialect. */
	private final JDBCBocasDialect dialect;
	/** Loader to use for values. */
	private final MemoryByteSourceLoader loader;

	private static void checkKey(ByteString key) {
		checkNotNull(key, "The object key must be provided");
	}

	private static void checkKeys(Iterable<ByteString> key) {
		checkNotNull(key, "The object keys must be provided");
	}

	/** Constructor. */
	JDBCBocas(BocasHashFunction function, DataSource dataSource, JDBCBocasDialect dialect, MemoryByteSourceLoader loader) {
		super(function);
		this.dataSource = checkNotNull(dataSource, "The data source must be provided");
		this.dialect = checkNotNull(dialect, "The data source dialect must be provided");
		this.loader = checkNotNull(loader, "The value loader must be provided");
	}
	
	@Override
	public void close() {
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
			Boolean perform() throws SQLException {
				return contains(key);
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
			Set<ByteString> perform() throws SQLException {
				Set<ByteString> set = Sets.newHashSet();
				for (ByteString key : keys) {
					checkKey(key);
					if (!set.contains(key) && contains(key)) {
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
	public Optional<ByteSource> get(final ByteString key) {
		checkKey(key);
		return new Tx<Optional<ByteSource>>() {
			@Override
			Optional<ByteSource> perform() throws SQLException {
				return read(key);
			}
		}.run();
	}

	/*
	 * (non-Javadoc)
	 * @see net.derquinse.bocas.Bocas#get(java.lang.Iterable)
	 */
	@Override
	public Map<ByteString, ByteSource> get(final Iterable<ByteString> keys) {
		checkKeys(keys);
		return new Tx<Map<ByteString, ByteSource>>() {
			@Override
			Map<ByteString, ByteSource> perform() throws SQLException {
				Map<ByteString, ByteSource> map = Maps.newHashMap();
				for (ByteString key : keys) {
					checkKey(key);
					if (!map.containsKey(key)) {
						Optional<ByteSource> v = read(key);
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
	 * com.google.common.io.ByteSource)
	 */
	@Override
	protected void put(final ByteString key, final ByteSource value) {
		new Put() {
			@Override
			void put() throws SQLException {
				write(key, value);
			}
		}.run();
	}

	/*
	 * (non-Javadoc)
	 * @see net.derquinse.bocas.SkeletalBocas#putAll(java.util.Map)
	 */
	@Override
	protected void putAll(final Map<ByteString, ByteSource> entries) {
		new Put() {
			@Override
			void put() throws SQLException {
				for (Entry<ByteString, ByteSource> entry : entries.entrySet()) {
					write(entry.getKey(), entry.getValue());
				}
			}
		}.run();
	}

	/** Bocas JDBC transaction. */
	private abstract class Tx<T> {
		private Connection cnn;

		final T run() {
			try {
				cnn = dataSource.getConnection();
				try {
					return runTx();
				} finally {
					cnn.close();
					cnn = null;
				}
			} catch (SQLException e) {
				throw new BocasException(e);
			}
		}

		private T runTx() throws SQLException {
			cnn.setAutoCommit(false);
			boolean ok = false;
			try {
				T value = perform();
				ok = true;
				return value;
			} finally {
				if (ok) {
					cnn.commit();
				} else {
					cnn.rollback();
				}
			}
		}

		/**
		 * Checks whether an entry is contained in the database.
		 * @param key Key to check.
		 * @return True if the entry is found.
		 */
		final boolean contains(ByteString key) throws SQLException {
			final PreparedStatement ps = cnn.prepareStatement("SELECT BOCAS_KEY FROM BOCAS_TABLE WHERE BOCAS_KEY = ?");
			try {
				dialect.setKey(ps, 1, key);
				final ResultSet rs = ps.executeQuery();
				try {
					return rs.next();
				} finally {
					rs.close();
				}
			} finally {
				ps.close();
			}
		}

		/**
		 * Reads an entry from the database.
		 * @param key Key to read.
		 * @return The entry value if found.
		 */
		final Optional<ByteSource> read(ByteString key) throws SQLException {
			final PreparedStatement ps = cnn.prepareStatement("SELECT BOCAS_VALUE FROM BOCAS_TABLE WHERE BOCAS_KEY = ?");
			try {
				dialect.setKey(ps, 1, key);
				final ResultSet rs = ps.executeQuery();
				try {
					if (rs.next()) {
						ByteSource value = dialect.getValue(rs, 1, loader);
						return Optional.of(value);
					}
					return Optional.absent();
				} finally {
					rs.close();
				}
			} finally {
				ps.close();
			}
		}

		/**
		 * Writes an entry if it does not exist.
		 * @param key Entry key.
		 * @param value Entry value.
		 */
		final void write(ByteString key, ByteSource value) throws SQLException {
			if (contains(key))
				return;
			final PreparedStatement ps = cnn.prepareStatement("INSERT INTO BOCAS_TABLE(BOCAS_KEY, BOCAS_VALUE) VALUES (?,?)");
			try {
				dialect.setKey(ps, 1, key);
				dialect.setValue(ps, 2, value);
				ps.executeUpdate();
			} finally {
				ps.close();
			}
		}

		abstract T perform() throws SQLException;
	}

	/** Put transactions. */
	private abstract class Put extends Tx<Object> {
		@Override
		final ByteString perform() throws SQLException {
			put();
			return null;
		}

		abstract void put() throws SQLException;
	}

}
