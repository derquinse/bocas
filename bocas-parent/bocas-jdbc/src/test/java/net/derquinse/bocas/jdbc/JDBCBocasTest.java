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

import java.sql.Connection;

import javax.sql.DataSource;

import net.derquinse.bocas.Bocas;
import net.derquinse.bocas.BocasExerciser;
import net.derquinse.common.test.h2.H2MemorySingleConnectionDataSource;

import org.springframework.jdbc.datasource.SingleConnectionDataSource;
import org.testng.annotations.Test;

/**
 * Test for JDBCBocas.
 * @author Andres Rodriguez.
 */
public class JDBCBocasTest {
	public JDBCBocasTest() {
	}

	private void test(Bocas bocas) throws Exception {
		try {
			BocasExerciser.exercise(bocas, 0);
		} finally {
			bocas.close();
		}
	}

	@Test
	public void h2() throws Exception {
		DataSource ds = new H2MemorySingleConnectionDataSource();
		Connection cnn = ds.getConnection();
		cnn.createStatement().execute("CREATE TABLE BOCAS_TABLE(BOCAS_KEY BINARY(32) PRIMARY KEY, BOCAS_VALUE BLOB)");
		cnn.close();
		test(JDBCBocasServices.newBuilder().build(ds));
	}

	//@Test
	public void mysql() throws Exception {
		DataSource ds = new SingleConnectionDataSource("url", "user", "passwd", true);
		Connection cnn = ds.getConnection();
		cnn.createStatement().execute("CREATE TABLE BOCAS_TABLE(BOCAS_KEY BINARY(32) PRIMARY KEY, BOCAS_VALUE LONGBLOB)");
		cnn.close();
		test(JDBCBocasServices.newBuilder().build(ds));
	}

}
