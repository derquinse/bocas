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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import net.derquinse.common.base.ByteString;
import net.derquinse.common.io.MemoryByteSource;
import net.derquinse.common.io.MemoryByteSourceLoader;

import com.google.common.annotations.Beta;
import com.google.common.io.ByteSource;

/**
 * JDBC Dialects for Bocas repositories.
 * @author Andres Rodriguez.
 */
@Beta
public enum JDBCBocasDialect {
	MYSQL {
		@Override
		InputStream loadValue(ResultSet rs, int index) throws SQLException {
			return new ByteArrayInputStream(rs.getBytes(index));
		}

	},

	PGSQL {
		@Override
		InputStream loadValue(ResultSet rs, int index) throws SQLException {
			return rs.getBinaryStream(index);
		}

	},

	H2 {
		@Override
		InputStream loadValue(ResultSet rs, int index) throws SQLException {
			return rs.getBinaryStream(index);
		}

		@Override
		void setValue(PreparedStatement ps, int index, ByteSource value) throws SQLException {
			try {
				ps.setBinaryStream(index, new ByteArrayInputStream(value.read()));
			} catch (IOException e) {
				throw new SQLException(e);
			}
		}
	};

	/** Reads a key from a result set. */
	ByteString getKey(ResultSet rs, int index) throws SQLException {
		return ByteString.copyFrom(rs.getBytes(index));
	}

	/** Reads a value from a result set. */
	final MemoryByteSource getValue(ResultSet rs, int index, MemoryByteSourceLoader loader) throws SQLException {
		try {
			return loader.load(loadValue(rs, index));
		} catch (IOException e) {
			throw new SQLException(e);
		}
	}

	/** Loads a value from a result set (internal). */
	abstract InputStream loadValue(ResultSet rs, int index) throws SQLException;

	/** Puts a key in a prepared statement parameter. */
	void setKey(PreparedStatement ps, int index, ByteString key) throws SQLException {
		ps.setBytes(index, key.toByteArray());
	}

	/** Puts a value in a prepared statement parameter. */
	void setValue(PreparedStatement ps, int index, ByteSource value) throws SQLException {
		try {
			ps.setBytes(index, value.read());
		} catch (IOException e) {
			throw new SQLException(e);
		}
	}

}
