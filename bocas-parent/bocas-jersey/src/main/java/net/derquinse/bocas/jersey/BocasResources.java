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
package net.derquinse.bocas.jersey;

import com.google.common.base.CharMatcher;
import com.google.common.base.Splitter;

import net.derquinse.common.base.NotInstantiable;

/**
 * Resource constants for Bocas RESTful API.
 * @author Andres Rodriguez.
 */
public final class BocasResources extends NotInstantiable {
	private BocasResources() {
	}
	
	/** Key splitter. */
	public static final Splitter KEY_SPLITTER = Splitter.on(CharMatcher.WHITESPACE).trimResults().omitEmptyStrings();

	/** Creator resource. */
	public static final String FUNNEL = "funnel";

	/** Service catalog resource. */
	public static final String CATALOG = "catalog";

}
