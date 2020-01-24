package testing;

import app_kvServer.DSCache;
import app_kvServer.Disk;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import junit.framework.TestCase;
import org.junit.rules.Timeout;

import java.util.*;

public class AdditionalTest extends TestCase {

	/**
	 * (1) data consistency test
	 * (2) crash test - check if all data is persisted to disk if server crashes
	 */

	@Rule
	public Timeout globalTimeout = new Timeout(10000);

	public void tearDown() {
		Disk.clearStorage();
	}

	@Test
	public void testCacheFunc() throws Exception {
		DSCache dsCache = new DSCache(100, "FIFO");
		dsCache.putKV("1", "1265309548");
		dsCache.putKV("2", "9665117208");
		dsCache.putKV("3", "3979847452");
		dsCache.putKV("4", "6531077644");
		dsCache.putKV("5", "6853866846");
		dsCache.putKV("6", "0802567709");

		assertEquals(dsCache.getKV("1"), "1265309548");
		assertEquals(dsCache.getKV("2"), "9665117208");
		assertEquals(dsCache.getKV("3"), "3979847452");
		assertEquals(dsCache.getKV("4"), "6531077644");
		assertEquals(dsCache.getKV("5"), "6853866846");
		assertEquals(dsCache.getKV("6"), "0802567709");
		assertEquals(dsCache.getCacheSize(), 6);
	}

	@Test
	public void testCacheLRU() throws Exception {
		DSCache dsCache = new DSCache(4, "LRU");
		// Fill up cache
		dsCache.putKV("1", "1265309548"); // t=0
		dsCache.putKV("2", "9665117208"); // t=1
		dsCache.putKV("3", "3979847452"); // t=2
		dsCache.putKV("4", "6531077644"); // t=3

		// Update lastModified for a few entries
		dsCache.getKV("1"); // t=4
		dsCache.getKV("2"); // t=5

		/*
		 * Cache Layout: (let t=0 at start)
		 * ---------------------------------------------------------
		 * key 	Value		lastModified	accessFrequency		order
		 * ---------------------------------------------------------
		 * 1	1265309548	4				2					1
		 * 2	9665117208	5				2					2
		 * 3	3979847452	2				1					3 <- evict
		 * 4	6531077644	3				1					4 <- evict
		 */

		dsCache.putKV("5", "6853866846"); // t=6; should evict "3"

		assertEquals(dsCache.getCacheSize(), 4);
		assertTrue(!dsCache.inCache("3"));
		assertTrue(dsCache.inCache("5"));

		dsCache.putKV("6", "0802567709"); // t=7; should evict "4"

		/*
		 * Cache Layout: (let t=0 at start)
		 * ---------------------------------------------------------
		 * key 	Value		lastModified	accessFrequency		order
		 * ---------------------------------------------------------
		 * 1	1265309548	4				2					1
		 * 2	9665117208	5				2					2
		 * 5	6853866846	6				1					5 <- new
		 * 6	0802567709	7				1					6 <- new
		 */

		assertEquals(dsCache.getCacheSize(), 4);
		assertTrue(!dsCache.inCache("3"));
		assertTrue(!dsCache.inCache("4"));
		assertTrue(dsCache.inCache("1"));
		assertTrue(dsCache.inCache("2"));
		assertTrue(dsCache.inCache("5"));
		assertTrue(dsCache.inCache("6"));

		// Finally, bring back evicted entries from Disk
		assertEquals(dsCache.getKV("3"), "3979847452");
		assertEquals(dsCache.getKV("4"), "6531077644");
	}

	@Test
	public void testCacheFIFO() throws Exception {
		DSCache dsCache = new DSCache(4, "FIFO");
		// Fill up cache
		dsCache.putKV("1", "1265309548"); // t=0
		dsCache.putKV("2", "9665117208"); // t=1
		dsCache.putKV("3", "3979847452"); // t=2
		dsCache.putKV("4", "6531077644"); // t=3

		// Update lastModified for a few entries
		dsCache.getKV("1"); // t=4
		dsCache.getKV("2"); // t=5

		/*
		 * Cache Layout: (let t=0 at start)
		 * ---------------------------------------------------------
		 * key 	Value		lastModified	accessFrequency		order
		 * ---------------------------------------------------------
		 * 1	1265309548	4				2					1 <- evict
		 * 2	9665117208	5				2					2 <- evict
		 * 3	3979847452	2				1					3
		 * 4	6531077644	3				1					4
		 */

		dsCache.putKV("5", "6853866846"); // t=6; should evict "1"

		assertEquals(dsCache.getCacheSize(), 4);
		assertTrue(!dsCache.inCache("1"));
		assertTrue(dsCache.inCache("5"));

		dsCache.putKV("6", "0802567709"); // t=7; should evict "2"

		/*
		 * Cache Layout: (let t=0 at start)
		 * ---------------------------------------------------------
		 * key 	Value		lastModified	accessFrequency		order
		 * ---------------------------------------------------------
		 * 5	6853866846	6				1					5 <- new
		 * 6	0802567709	7				1					6 <- new
		 * 3	3979847452	2				1					3
		 * 4	6531077644	3				1					4
		 */

		assertEquals(dsCache.getCacheSize(), 4);
		assertTrue(!dsCache.inCache("1"));
		assertTrue(!dsCache.inCache("2"));
		assertTrue(dsCache.inCache("3"));
		assertTrue(dsCache.inCache("4"));
		assertTrue(dsCache.inCache("5"));
		assertTrue(dsCache.inCache("6"));

		// Finally, bring back evicted entries from Disk
		assertEquals(dsCache.getKV("1"), "1265309548");
		assertEquals(dsCache.getKV("2"), "9665117208");
	}

	@Test
	public void testCacheLFU() throws Exception {
		DSCache dsCache = new DSCache(4, "LFU");
		// Fill up cache
		dsCache.putKV("1", "1265309548"); // t=0
		dsCache.putKV("2", "9665117208"); // t=1
		dsCache.putKV("3", "3979847452"); // t=2
		dsCache.putKV("4", "6531077644"); // t=3

		// Update lastModified for a few entries
		dsCache.getKV("1"); // t=4
		dsCache.getKV("1"); // t=5
		dsCache.getKV("1"); // t=6
		dsCache.getKV("1"); // t=7
		dsCache.getKV("1"); // t=8

		dsCache.getKV("2"); // t=9
		dsCache.getKV("2"); // t=10
		dsCache.getKV("2"); // t=11

		dsCache.getKV("3"); // t=12
		dsCache.getKV("3"); // t=13

		dsCache.getKV("4"); // t=14

		/*
		 * Notice we've set up the cache so that the Most-Recently
		 * Used entry is also the Least Frequently Used: Take "4"
		 * for example, in LRU, "4" would not be evicted. but in LFU
		 * "4" is prime candidate because it has only been accessed 2
		 * times while other entries have been accessed more than 2.
		 *
		 * Conversely, "1", who would be prime candidate to be evicted
		 * in LRU is last-in-line to be evicted in LFU since it is the
		 * most frequently accessed entry by far.
		 *
		 * We expect, therefore, that "4" be evicted first. Since the
		 * replaced entry will have accessFrequency of 1, this new
		 * entry will be prime candidate to be evicted again since it
		 * is LFU. As shown below, we expect both evictions to come from
		 * the same cache entry.
		 *
		 * Cache Layout: (let t=0 at start)
		 * ---------------------------------------------------------
		 * key 	Value		lastModified	accessFrequency		order
		 * ---------------------------------------------------------
		 * 1	1265309548	8				6					1
		 * 2	9665117208	11				4					2
		 * 3	3979847452	13				3					3
		 * 4	6531077644	14				2					4 <- evict <- evict
		 */

		dsCache.putKV("5", "6853866846"); // t=15; should evict "4"

		assertEquals(dsCache.getCacheSize(), 4);
		assertTrue(!dsCache.inCache("1"));
		assertTrue(dsCache.inCache("5"));

		dsCache.putKV("6", "0802567709"); // t=16; should evict "5"

		/*
		 * Cache Layout: (let t=0 at start)
		 * ---------------------------------------------------------
		 * key 	Value		lastModified	accessFrequency		order
		 * ---------------------------------------------------------
		 * 1	1265309548	8				6					1
		 * 2	9665117208	11				4					2
		 * 3	3979847452	13				3					3
		 * 6	6531077644	16				1					6 <- new (evicted "5" at t=16)
		 */

		assertEquals(dsCache.getCacheSize(), 4);
		assertTrue(!dsCache.inCache("5"));
		assertTrue(!dsCache.inCache("4"));
		assertTrue(dsCache.inCache("1"));
		assertTrue(dsCache.inCache("2"));
		assertTrue(dsCache.inCache("3"));
		assertTrue(dsCache.inCache("6"));

		// Finally, bring back evicted entries from Disk
		assertEquals(dsCache.getKV("5"), "6853866846");
		assertEquals(dsCache.getKV("4"), "6531077644");
	}

	@Test
	public void testCacheToDisk() throws Exception {
		// Choose arbitrary replacement strategy (any will do)
		DSCache dsCache = new DSCache(1, "FIFO");
		String key1 = "1";
		String value1 = UUID.randomUUID().toString();
		String key2 = "2";
		String value2 = UUID.randomUUID().toString();

		dsCache.putKV(key1, value1); // on disk
		dsCache.putKV(key2, value2); // in cache

		/*
		 * Alternate access to whichever entry is not in cache.
		 * Expects cache to "bring back" this entry from disk.
		 * Then update the value, and store it into cache. The
		 * purpose of this is: once the other entry inevitably
		 * evicts this entry, we want to check if the disk is
		 * storing the updated data (this is checked in the next
		 * iteration when this entry is brought back into cache).
		 */
		int i;
		String _val;
		for (i=0; i<1000; i++) {
			if (!dsCache.inCache(key1)) {
				_val = dsCache.getKV(key1);
				if (!_val.equals(value1)) {
					System.out.println(String.format(
						"GET key: %s, Expecting: %s, Actual: %s",
						key1, value1, _val
					));
				}
				assertEquals(_val, value1);
				assertTrue(dsCache.inCache(key1));
				value1 = UUID.randomUUID().toString();

				dsCache.putKV(key1, value1);
			} else {
				_val = dsCache.getKV(key2);
				if (!_val.equals(value2)) {
					System.out.println(String.format(
						"GET key: %s, Expecting: %s, Actual: %s",
						key2, value2, _val
					));
				}
				assertEquals(_val, value2);
				assertTrue(dsCache.inCache(key2));
				value2 = UUID.randomUUID().toString();

				dsCache.putKV(key2, value2);
			}
		}
	}

	private class CacheWorker extends Thread {
		final DSCache cache;
		final List<String> keys;
		static final int RAND_SEED = 10;
		final boolean isRead; /* if false, then write */

		CacheWorker(final DSCache dsCache, List<String> _keys, boolean _isRead) {
			cache = dsCache;
			keys = _keys;
			isRead = _isRead;
		}

		@Override
		public void run() {
			int i;
			String key;
			Random srand = new Random(RAND_SEED);
			for (i = 0; i < 200; i++) {
				try {
					key = keys.get(srand.nextInt(keys.size()));
					if (isRead) {
						cache.getKV(key);
					} else {
						cache.putKV(key, UUID.randomUUID().toString());
					}
				} catch (Exception | Error e) {
					System.out.printf("Failure detected for Thread-%d\n", getId());
					return;
				}
			}
		}
	}

	@Test
	public void testCacheThreadSafety() {
		/*
		 * Spawn 20 threads to update the same element with
		 * different values.
		 *
		 * Spawn 20 threads to concurrently evict that entry
		 * being updated.
		 *
		 * We are just trying to test for any deadlock scenarios,
		 * no real functionality is being tested here. Refer to
		 * above test cases for functionality.
		 */
		DSCache cache = new DSCache(100, "FIFO");
		List<String> keys = new ArrayList<>();
		int i;
		for (i=0; i<1000; i++) {
			keys.add(Integer.toString(i));
		}

		List<CacheWorker> workers = new ArrayList<>();

		/* Writers */
		CacheWorker writer;
		for (i=0; i<20; i++) {
			writer = new CacheWorker(cache, keys, false);
			writer.start();
			workers.add(writer);
		}

		/* Readers */
		CacheWorker reader;
		for (i=0; i<20; i++) {
			reader = new CacheWorker(cache, keys, true);
			reader.start();
			workers.add(reader);
		}

		/* Join */
		workers.forEach(worker -> {
			try {
				worker.join();
			} catch (InterruptedException e) {
				System.out.println("failure detected");
				fail();
			}
		});

		assertTrue(true);
	}

	@Test
	public void testDiskPutCreate() {
		/*
		 * Tests key-value pair insertion
		 * into persistent storage.
		 */
		String key;
		String value, _value;
		for (int i=0; i < 1000; i++) {
			key = Integer.toString(i);
			value = UUID.randomUUID().toString();

			Disk.putKV(key, value);
			_value = Disk.getKV(key);

			assertEquals(value, _value);
		}
	}

	@Test
	public void testDiskPutUpdate() {
		/*
		 * Tests key-value pair update requests
		 * Creates a single key-value pair and
		 * updates value for each iteration.
		 * Verify getKV(key) gets updated value.
		 */
		String key = "key";
		String value, _value;
		for (int i=0; i < 1000; i++) {
			value = UUID.randomUUID().toString();

			Disk.putKV(key, value);
			_value = Disk.getKV(key);

			assertEquals(value, _value);
		}
	}

	@Test
	public void testDiskGetNone() {
		/*
		 * Test Disk.getKV() for non-existent keys
		 * in persistent storage.
		 */
		String key = "random_key";
		String _value;
		for (int i=0; i < 1000; i++) {
			_value = Disk.getKV(key);

			assertNull(_value);
		}
	}

	@Test
	public void testClearCache() throws Exception {
		/*
		 * Verify all contents of cache are
		 * flushed to disk when cleared.
		 */
		DSCache dsCache = new DSCache(4, "FIFO");

		/* Fill up cache */
		dsCache.putKV("1", "one");
		dsCache.putKV("2", "one_two");
		dsCache.putKV("3", "one_two_three");
		dsCache.putKV("4", "one_two_three_four");

		/* Clear contents */
		dsCache.clearCache();

		/* Verify contents are persisted */
		assertEquals("one", Disk.getKV("1"));
		assertEquals("one_two", Disk.getKV("2"));
		assertEquals("one_two_three", Disk.getKV("3"));
		assertEquals("one_two_three_four", Disk.getKV("4"));
	}
}
