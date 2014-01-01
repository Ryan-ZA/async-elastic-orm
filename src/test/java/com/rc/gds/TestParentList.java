package com.rc.gds;

import java.util.List;

import com.rc.gds.annotation.ID;

public class TestParentList {

	@ID
	String id;

	String name;
	List<TestChild> testChildList;

}
