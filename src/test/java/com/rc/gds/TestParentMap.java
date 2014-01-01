package com.rc.gds;

import java.util.Map;

import com.rc.gds.annotation.ID;

public class TestParentMap {

	@ID
	String id;

	String name;
	Map<String, TestChild> testChildMap;

}
