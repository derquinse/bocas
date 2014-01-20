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
package net.derquinse.bocas;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import net.derquinse.common.base.ByteString;
import net.derquinse.common.io.MemoryByteSource;
import net.derquinse.common.io.MemoryByteSourceLoader;
import net.derquinse.common.test.RandomSupport;
import net.derquinse.common.util.zip.MaybeCompressed;
import net.derquinse.common.util.zip.ZipFileLoader;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.ByteSource;
import com.google.common.primitives.Longs;

/**
 * Bocas bucket exerciser.
 */
public final class BocasExerciser {
	/** New data index. */
	private static final AtomicLong INDEX = new AtomicLong(0L);

	/** Repository. */
	private final Bocas bocas;
	/** Error flag. */
	private boolean ok = true;
	/** Number of tasks. */
	private final int tasks;

	public static MemoryByteSource data() {
		byte[] deterministic = Longs.toByteArray(INDEX.incrementAndGet());
		byte[] data = RandomSupport.getBytes(RandomSupport.nextInt(1024, 10240));
		for (int i = 0; i < deterministic.length; i++) {
			data[i] = deterministic[i];
		}
		return MemoryByteSource.wrap(data);
	}

	public static Map<ByteString, MemoryByteSource> dataSet(BocasHashFunction f, int size) {
		Map<ByteString, MemoryByteSource> map = Maps.newHashMapWithExpectedSize(size);
		for (int i = 0; i < size; i++) {
			MemoryByteSource v = data();
			map.put(f.hash(v), v);
		}
		return map;
	}

	public static void check(ByteSource value, ByteSource data) throws IOException {
		assertEquals(data.read(), value.read());
	}

	/** Exercises a Bocas repository. */
	public static void exercise(Bocas bocas, int tasks) throws Exception {
		BocasExerciser e = new BocasExerciser(bocas, tasks);
		e.run();
	}

	/** Exercises a Bocas repository. */
	public static void exercise(Bocas bocas) throws Exception {
		exercise(bocas, 1000);
	}

	/** Exercise a Bocas repository putting a cache in front of it. */
	public static void cached(BocasService service, String bucketName) throws Exception {
		final Bocas bucket = service.getBucket(bucketName);
		// Heap cache
		BocasService cache = BocasServices.cache().expireAfterAccess(10L, TimeUnit.MINUTES).maximumSize(1000)
				.build(service);
		Bocas cached = cache.getBucket(bucketName);
		exercise(cached);
		MemoryByteSource value = data();
		ByteString key = bucket.put(value);
		assertTrue(cached.contains(key));
		// Direct cache
		cache = BocasServices.cache().expireAfterAccess(10L, TimeUnit.MINUTES).maximumWeight(10000000L)
				.loader(MemoryByteSourceLoader.get().direct(true)).build(service);
		cached = cache.getBucket(bucketName);
		exercise(cached);
		value = data();
		key = bucket.put(value);
		assertTrue(cached.contains(key));
	}

	/** Exercise a Bocas repository putting it as the seed of a memory one. */
	public static void fallback(BocasService service, String bucketName) throws Exception {
		final Bocas bocas = service.getBucket(bucketName);
		final Bocas primary = BocasServices.memoryBucket(bocas.getHashFunction(), MemoryByteSourceLoader.get().merge(true));
		final Bocas fallback = BocasServices.seeded(primary, bocas);
		BocasExerciser.exercise(fallback);
		ByteSource value = data();
		ByteString key = bocas.getHashFunction().hash(value);
		assertFalse(primary.contains(key));
		assertFalse(fallback.contains(key));
		bocas.put(value);
		assertTrue(bocas.contains(key));
		assertFalse(primary.contains(key));
		assertTrue(fallback.contains(key));
	}

	private BocasExerciser(Bocas bocas, int tasks) {
		this.bocas = checkNotNull(bocas, "The repository to exercise must be provided");
		this.tasks = Math.max(10, tasks);
	}

	private ByteString hash(ByteSource value) {
		return bocas.getHashFunction().hash(value);
	}

	private void checkInRepository(ByteSource value) throws IOException {
		ByteString k = hash(value);
		assertTrue(bocas.contains(k));
		assertTrue(bocas.contained(ImmutableSet.of(k)).contains(k));
		Optional<ByteSource> optional = bocas.get(k);
		assertTrue(optional.isPresent());
		check(value, optional.get());
		Map<ByteString, ByteSource> map = bocas.get(ImmutableSet.of(k));
		assertTrue(map.containsKey(k));
		// Check with repeatable read
		for (int i = 0; i < 5; i++) {
			check(value, map.get(k));
		}
	}

	private ByteString put(ByteSource value) throws IOException {
		ByteString returned = bocas.put(value);
		ByteString k = hash(value);
		assertEquals(returned, k);
		checkInRepository(value);
		return k;
	}

	private void checkNotInRepository(ByteSource value) throws Exception {
		ByteString k = hash(value);
		assertFalse(bocas.contains(k));
		assertTrue(bocas.contained(ImmutableSet.of(k)).isEmpty());
		assertFalse(bocas.get(k).isPresent());
		assertTrue(bocas.get(ImmutableSet.of(k)).isEmpty());
	}

	/** Exercise the repository. */
	private void run() throws Exception {
		MemoryByteSource data1 = data();
		ByteString k1 = put(data1);
		MemoryByteSource data2 = data();
		ByteString k2 = put(data2);
		MemoryByteSource data3 = data();
		ByteString k3 = hash(data3);
		checkInRepository(data1);
		checkNotInRepository(data3);
		MemoryByteSource data4 = data();
		ByteString k4 = hash(data4);
		List<ByteString> list = ImmutableList.of(k1, k2, k3, k4, k1);
		assertEquals(bocas.contained(list).size(), 2);
		assertEquals(bocas.get(list).size(), 2);
		List<ByteString> keys = bocas.putAll(ImmutableList.of(data2, data3));
		assertEquals(keys.size(), 2);
		assertEquals(keys.get(0), k2);
		assertEquals(keys.get(1), k3);
		checkInRepository(data1);
		checkInRepository(data2);
		checkInRepository(data3);
		checkNotInRepository(data4);
		assertEquals(bocas.contained(list).size(), 3);
		assertEquals(bocas.get(list).size(), 3);
		// Multiple operation - Phase I
		List<ByteString> keyList = Lists.newLinkedList();
		List<MemoryByteSource> valueList = Lists.newLinkedList();
		for (int i = 0; i < 15; i++) {
			MemoryByteSource e = data();
			keyList.add(hash(e));
			valueList.add(e);
		}
		bocas.putAll(valueList);
		assertTrue(bocas.contained(keyList).containsAll(keyList));
		assertTrue(bocas.get(keyList).keySet().containsAll(keyList));
		// Multiple operation - Phase II
		for (int i = 0; i < 15; i++) {
			MemoryByteSource e = data();
			keyList.add(hash(e));
			valueList.add(e);
		}
		bocas.putAll(valueList);
		assertTrue(bocas.contained(keyList).containsAll(keyList));
		assertTrue(bocas.get(keyList).keySet().containsAll(keyList));
		// ZIP
		ZipBocas zb = ZipBocas.of(bocas);
		Map<String, ByteString> entries = zb.putZip(ZipFileLoader.get().load(getClass().getResourceAsStream("loren.zip")));
		assertEquals(bocas.contained(entries.values()).size(), 3);
		// ZIP
		Map<String, MaybeCompressed<ByteString>> mentries = zb.putZipAndGZip(ZipFileLoader.get().load(
				getClass().getResourceAsStream("loren.zip")));
		assertEquals(bocas.contained(entries.values()).size(), 3);
		for (MaybeCompressed<ByteString> mk : mentries.values()) {
			assertTrue(mk.isCompressed());
		}
		// Concurrent operation.
		if (tasks > 0) {
			ExecutorService s = Executors.newFixedThreadPool(5);
			for (int i = 0; i < tasks; i++) {
				s.submit(new Task());
			}
			s.shutdown();
			while (!s.awaitTermination(5, TimeUnit.SECONDS))
				;
			assertTrue(ok);
		}
	}

	private class Task implements Runnable {
		@Override
		public void run() {
			try {
				put(data());
			} catch (Exception e) {
				ok = false;
				throw new RuntimeException(e);
			}
		}
	}

}
