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

import javax.ws.rs.Path;

/**
 * Integration test for Jersey Bocas service.
 * @author Andres Rodriguez
 */
@Path("/")
public class TestBocasResource extends BocasServiceResource {
	public TestBocasResource() {
		super(BocasJerseyTest.SERVER);
	}
}
