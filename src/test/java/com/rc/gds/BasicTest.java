package com.rc.gds;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;

import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.rc.gds.TestSubClass.TheSubClass;
import com.rc.gds.interfaces.GDSResult;

public class BasicTest {

	private static GDS getGDS() {
		return new GDS("gdstest");
	}

	@Before
	public void testSetup() {
		try {
			getGDS().client.admin().indices().prepareDelete().execute().actionGet();
		} catch (Exception ex) {
		}
		getGDS();
	}

	@After
	public void testCleanup() {
		try {
			Thread.sleep(50);
			getGDS().client.admin().indices().prepareDelete().execute().actionGet();
		} catch (Exception ex) {
		}
	}

	@Test
	public void testEmbed() {
		TestEmbedHolder embedHolder = new TestEmbedHolder();
		embedHolder.testEmbed1 = new TestEmbed();
		embedHolder.testEmbed1.x = 99;
		embedHolder.testEmbed1.y = 12;
		embedHolder.testEmbed2 = new TestEmbed();
		embedHolder.testEmbed2.x = 1;
		embedHolder.testEmbed2.y = 2;
		embedHolder.testEmbed2.z = 99L;
		
		embedHolder.testEmbed1.insideEmbed = new TestEmbed();
		embedHolder.testEmbed1.insideEmbed.zz = 109L;
		
		getGDS().save().entity(embedHolder).now();
		
		refreshIndex();
		TestEmbedHolder loaded = getGDS().query(TestEmbedHolder.class).asList().get(0);
		Assert.assertEquals(loaded.testEmbed1.x, 99);
		Assert.assertEquals(loaded.testEmbed2.y, new Integer(2));
		Assert.assertEquals(loaded.testEmbed2.z, 99L);
		Assert.assertEquals(loaded.testEmbed1.insideEmbed.zz, new Long(109L));
	}

	@Test
	public void testEmbedList() {
		TestEmbedListHolder embedHolder = new TestEmbedListHolder();
		
		for (int i = 0; i < 25; i++) {
			TestEmbed embed = new TestEmbed();
			embed.x = i;
			embedHolder.testEmbedList.add(embed);
		}
		
		getGDS().save().entity(embedHolder).now();
		
		refreshIndex();
		TestEmbedListHolder loaded = getGDS().query(TestEmbedListHolder.class).asList().get(0);
		
		for (int i = 0; i < 25; i++) {
			TestEmbed embed = loaded.testEmbedList.get(i);
			Assert.assertEquals(i, embed.x);
			Assert.assertEquals(null, embed.insideEmbed);
		}
	}

	@Test
	public void testEmbedMap() {
		TestEmbedMapHolder embedHolder = new TestEmbedMapHolder();
		
		for (int i = 0; i < 25; i++) {
			TestEmbed embed = new TestEmbed();
			embed.x = i;
			embedHolder.testEmbedMap.put("key" + i, embed);
		}
		
		getGDS().save().entity(embedHolder).now();
		
		refreshIndex();
		TestEmbedMapHolder loaded = getGDS().query(TestEmbedMapHolder.class).asList().get(0);
		
		for (int i = 0; i < 25; i++) {
			TestEmbed embed = loaded.testEmbedMap.get("key" + i);
			Assert.assertEquals(i, embed.x);
			Assert.assertEquals(null, embed.insideEmbed);
		}
	}

	@Test
	public void testList() {

		Random random = new Random();
		int num = random.nextInt();

		TestParentList testParentList = new TestParentList();
		testParentList.name = "testParentList" + num;
		testParentList.testChildList = new ArrayList<>();
		for (int i = 0; i < 25; i++) {
			TestChild testChild = new TestChild();
			testChild.name = "child" + i;
			testParentList.testChildList.add(testChild);
		}

		getGDS().save().entity(testParentList).now();
		
		refreshIndex();

		TestParentList fetchParent = getGDS().load().fetch(TestParentList.class, testParentList.id).now();

		for (int i = 0; i < 25; i++) {
			Assert.assertNotNull(fetchParent.testChildList.get(i).name);
		}
		
		Assert.assertEquals(25, fetchParent.testChildList.size());

	}

	@Test
	public void testMap() {
		
		Random random = new Random();
		int num = random.nextInt();
		
		TestParentMap testParentMap = new TestParentMap();
		testParentMap.name = "testParentList" + num;
		testParentMap.testChildMap = new HashMap<String, TestChild>();
		for (int i = 0; i < 25; i++) {
			TestChild testChild = new TestChild();
			testChild.name = "child" + i;
			testParentMap.testChildMap.put("key" + i, testChild);
		}
		
		getGDS().save().entity(testParentMap).now();
		
		TestParentMap fetchParent = getGDS().load().fetch(TestParentMap.class, testParentMap.id).now();
		Assert.assertEquals(25, fetchParent.testChildMap.size());
		
		for (int i = 0; i < 25; i++) {
			Assert.assertEquals("child" + i, fetchParent.testChildMap.get("key" + i).name);
		}
	}

	@Test
	public void testBasicMaps() {
		TestBasicMap map = new TestBasicMap();
		map.name = "bla";
		map.testMap = new HashMap<>();
		map.testMap.put("Test1", 332.131);
		map.testMap.put("t2", -8.0);

		getGDS().save().entity(map).now();

		TestBasicMap map2 = getGDS().load().fetch(TestBasicMap.class, map.id).now();

		Assert.assertEquals(map.testMap.size(), map2.testMap.size());
		Assert.assertEquals(map.testMap.get("Test1"), map2.testMap.get("Test1"));
		Assert.assertEquals(map.testMap.get("t2"), map2.testMap.get("t2"));

		map.testMap.put("z", 5.5);

		Assert.assertEquals(null, map2.testMap.get("z"));
	}

	@Test
	public void testPoly() {
		TestParentPoly parentPoly = new TestParentPoly();
		parentPoly.param1 = 4;
		parentPoly.param2 = true;
		parentPoly.param3 = Long.MAX_VALUE;
		
		TestChildPoly childPoly = new TestChildPoly();
		parentPoly.testChild = childPoly;
		
		getGDS().save().entity(parentPoly).now();
		
		refreshIndex();
		TestParentPoly loadPoly = (TestParentPoly) getGDS().query(TestParent.class).asList().get(0);
		Assert.assertEquals(parentPoly.param1, loadPoly.param1);
		Assert.assertEquals(parentPoly.param2, loadPoly.param2);
		Assert.assertEquals(parentPoly.param3, loadPoly.param3);
		Assert.assertEquals(parentPoly.testChild.getClass(), loadPoly.testChild.getClass());
		
		TestChildPoly loadChildPoly1 = getGDS().query(TestChildPoly.class).asList().get(0);
		TestChildPoly loadChildPoly2 = (TestChildPoly) getGDS().query(TestChild.class).asList().get(0);
		
		Assert.assertEquals(loadChildPoly1.id, loadChildPoly2.id);
		Assert.assertEquals(loadChildPoly1.bytes.get(2), loadChildPoly2.bytes.get(2));
	}

	private void refreshIndex() {
		getGDS().client.admin().indices().prepareRefresh().execute().actionGet();
	}

	@Test
	public void testQuery() {
		Random random = new Random();
		int num = random.nextInt();
		
		TestParent testParent = new TestParent();
		TestChild testChild = new TestChild();
		
		testParent.name = "parent" + num;
		testChild.name = "child" + num;
		testParent.testChild = testChild;
		
		getGDS().save().entity(testParent).now();
		
		refreshIndex();

		List<TestChild> list = getGDS().query(TestChild.class)
				//.filter(FilterBuilders.termFilter("name", "child" + num))
				.asList();
		Assert.assertEquals(1, list.size());
		Assert.assertEquals(testChild.id, list.get(0).id);
		Assert.assertEquals(testChild.name, list.get(0).name);
	}

	@Test
	public void testQueryMultiple() {
		for (int i = 0; i < 1000; i++) {
			TestParent testParent = new TestParent();
			TestChild testChild = new TestChild();
			
			testParent.name = "parent" + i;
			testChild.name = "child" + i;
			testParent.testChild = testChild;
			
			getGDS().save().entity(testParent).now();
		}
		
		refreshIndex();
		List<TestChild> list = getGDS().query(TestChild.class).asList();
		Assert.assertEquals(1000, list.size());
		//Assert.assertEquals("child12", list.get(12).name);
		//Assert.assertEquals("child22", list.get(22).name);
	}

	@Test
	public void testSave() {
		TestParent testParent = new TestParent();
		TestChild testChild = new TestChild();

		testParent.name = "parent1";
		testChild.name = "child1";
		testParent.testChild = testChild;

		getGDS().save().entity(testParent).now();

		Assert.assertNotNull(testParent.id);
		Assert.assertNotNull(testChild.id);

		TestParent fetchParent = getGDS().load().fetch(TestParent.class, testParent.id).now();
		Assert.assertEquals(testParent.name, fetchParent.name);
		Assert.assertEquals(testParent.testChild.name, fetchParent.testChild.name);
	}

	@Test
	public void testSubClass() {
		TestSubClass test = new TestSubClass();
		test.name = "parent";
		test.theSubClass = new TheSubClass();
		test.theSubClass.i = 867;

		getGDS().save().entity(test).now();

		TestSubClass load = getGDS().load().fetch(TestSubClass.class, test.id).now();

		Assert.assertNotNull(load.theSubClass);

		Assert.assertEquals(867, load.theSubClass.i);
	}

	@Test
	public void testMultiSave() {
		TestParent testParent = new TestParent();
		TestChild testChild = new TestChild();
		
		testParent.name = "parent1";
		testChild.name = "child1";
		testParent.testChild = testChild;
		
		getGDS().save().entity(testParent).now();
		
		Assert.assertNotNull(testParent.id);
		Assert.assertNotNull(testChild.id);
		
		for (int i = 0; i < 100; i++) {
			TestParent fetchParent = getGDS().load().fetch(TestParent.class, testParent.id).now();
			Assert.assertEquals(testParent.name, fetchParent.name);
			Assert.assertEquals(testParent.testChild.name, fetchParent.testChild.name);
			
			getGDS().save().entity(fetchParent).now();
		}
	}

	@Test
	public void testAlwaysPersist() {
		TestParent testParent = new TestParent();
		TestChild testChild = new TestChild();

		testParent.name = "parent1";
		testChild.name = "child1";
		testParent.testChild = testChild;

		getGDS().save().entity(testParent).now();

		testChild.name = "child2";

		getGDS().save().entity(testParent).now();

		TestParent fetchParent = getGDS().load().fetch(TestParent.class, testParent.id).now();
		Assert.assertEquals(testParent.testChild.name, fetchParent.testChild.name);
	}
	
	@Test
	public void testDeep() {
		TestParent testParent = new TestParent();
		TestChildChild tc1 = new TestChildChild();
		TestChildChild tc2 = new TestChildChild();
		TestChildChild tc3 = new TestChildChild();
		TestChildChild tc4 = new TestChildChild();
		
		tc1.name = "tc1";
		tc1.deepChild = tc2;
		tc2.name = "tc2";
		tc2.deepChild = tc3;
		tc3.name = "tc3";
		tc3.deepChild = tc4;
		tc4.name = "tc4";

		testParent.name = "parent1";
		testParent.testChild = tc1;

		getGDS().save().entity(testParent).now();

		Assert.assertNotNull(testParent.id);
		Assert.assertNotNull(tc1.id);
		Assert.assertNotNull(tc2.id);
		Assert.assertNotNull(tc3.id);
		Assert.assertNotNull(tc4.id);

		TestParent fetchParent = getGDS().load().fetch(TestParent.class, testParent.id).now();
		TestChildChild f1 = (TestChildChild) fetchParent.testChild;
		TestChildChild f2 = f1.deepChild;
		TestChildChild f3 = f2.deepChild;
		TestChildChild f4 = f3.deepChild;
		
		Assert.assertEquals(tc1.name, f1.name);
		Assert.assertEquals(tc2.name, f2.name);
		Assert.assertEquals(tc3.name, f3.name);
		Assert.assertEquals(tc4.name, f4.name);
	}
	
	@Test
	public void testBidirectional() {
		TestChildChild child1 = new TestChildChild();
		child1.name = "test1";
		
		TestChildChild child2 = new TestChildChild();
		child2.name = "test2";
		
		child1.deepChild = child2;
		child2.deepChild = child1;
		
		getGDS().save().entity(child1).now();
		
		Assert.assertNotNull(child1.id);
		Assert.assertNotNull(child2.id);
		
		TestChildChild fetch1 = getGDS().load().fetch(TestChildChild.class, child1.id).now();
		
		Assert.assertEquals(child1.name, fetch1.name);
		Assert.assertEquals(child2.name, fetch1.deepChild.name);
		
		TestChildChild fetch2 = getGDS().load().fetch(TestChildChild.class, child2.id).now();
		
		Assert.assertEquals(child2.name, fetch2.name);
		Assert.assertEquals(child1.name, fetch2.deepChild.name);
	}
	
	@Test
	public void testSameObjectMultipleTimesInList() {
		Random random = new Random();
		int num = random.nextInt();
		
		TestParentList testParentList = new TestParentList();
		testParentList.name = "testParentList" + num;
		testParentList.testChildList = new ArrayList<>();
		for (int i = 0; i < 10; i++) {
			TestChild testChild = new TestChild();
			testChild.name = "child" + i;
			testParentList.testChildList.add(testChild);
			testParentList.testChildList.add(testChild);
			testParentList.testChildList.add(testChild);
		}
		
		getGDS().save().entity(testParentList).now();
		
		refreshIndex();
		
		TestParentList fetchParent = getGDS().load().fetch(TestParentList.class, testParentList.id).now();
		
		for (int i = 0; i < 30; i++) {
			Assert.assertNotNull(fetchParent.testChildList.get(i).name);
		}
		
		Assert.assertEquals(30, fetchParent.testChildList.size());
	}
	
	@Test
	public void testSameObjectMultipleTimesInMap() {
		Random random = new Random();
		int num = random.nextInt();
		
		TestParentMap testParentMap = new TestParentMap();
		testParentMap.name = "testParentList" + num;
		testParentMap.testChildMap = new HashMap<String, TestChild>();
		for (int i = 0; i < 10; i++) {
			TestChild testChild = new TestChild();
			testChild.name = "child" + i;
			testParentMap.testChildMap.put("key " + i, testChild);
			testParentMap.testChildMap.put("key2 " + i, testChild);
			testParentMap.testChildMap.put("key3 " + i, testChild);
		}
		
		getGDS().save().entity(testParentMap).now();
		
		Assert.assertNotNull(testParentMap.id);
		for (TestChild testChild : testParentMap.testChildMap.values())
			Assert.assertNotNull(testChild.id);

		TestParentMap fetchParent = getGDS().load().fetch(TestParentMap.class, testParentMap.id).now();
		Assert.assertEquals(30, fetchParent.testChildMap.size());

		for (int i = 0; i < 10; i++) {
			Assert.assertEquals("child" + i, fetchParent.testChildMap.get("key " + i).name);
			Assert.assertEquals("child" + i, fetchParent.testChildMap.get("key2 " + i).name);
			Assert.assertEquals("child" + i, fetchParent.testChildMap.get("key3 " + i).name);
		}
	}
	
	@Test
	public void testBatcher() {
		
		TestParent testParent1 = new TestParent();
		GDSResult<Key> result1 = getGDS().save().entity(testParent1).result();
		TestParent testParent2 = new TestParent();
		GDSResult<Key> result2 = getGDS().save().entity(testParent2).result();
		TestParent testParent3 = new TestParent();
		GDSResult<Key> result3 = getGDS().save().entity(testParent3).result();
		TestParent testParent4 = new TestParent();
		GDSResult<Key> result4 = getGDS().save().entity(testParent4).result();
		TestParent testParent5 = new TestParent();
		GDSResult<Key> result5 = getGDS().save().entity(testParent5).result();
		
		GDSResult<Boolean> allResult = new GDSBatcher(result1, result2, result3, result4, result5).onAllComplete();
		boolean success = allResult.now();
		
		Assert.assertEquals(true, success);
		
		Assert.assertNotNull(testParent1.id);
		Assert.assertNotNull(testParent2.id);
		Assert.assertNotNull(testParent3.id);
		Assert.assertNotNull(testParent4.id);
		Assert.assertNotNull(testParent5.id);
	}
	
	@Test
	public void testVersionGoodUpdates() {
		TestVersionedObject testVersionedObject = new TestVersionedObject();
		testVersionedObject.name = "one";
		Key key1 = getGDS().save(testVersionedObject).now();
		
		Assert.assertNotNull(testVersionedObject.id);
		Assert.assertEquals(1, testVersionedObject.ver);
		
		testVersionedObject.name = "two";
		Key key2 = getGDS().save(testVersionedObject).now();
		
		Assert.assertEquals(2, testVersionedObject.ver);
		Assert.assertEquals(key1.id, key2.id);
	}
	
	@Test(expected = VersionConflictEngineException.class)
	public void testVersionBadUpdates() {
		TestVersionedObject testVersionedObject = new TestVersionedObject();
		testVersionedObject.name = "one";
		Key key1 = getGDS().save(testVersionedObject).now();
		
		Assert.assertNotNull(testVersionedObject.id);
		Assert.assertEquals(1, testVersionedObject.ver);
		
		testVersionedObject.name = "two";
		Key key2 = getGDS().save(testVersionedObject).now();
		
		Assert.assertEquals(2, testVersionedObject.ver);
		Assert.assertEquals(key1.id, key2.id);
		
		testVersionedObject.ver = 1;
		getGDS().save(testVersionedObject).now();
	}

}
