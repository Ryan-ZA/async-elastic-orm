package com.rc.gds;

import com.rc.gds.annotation.ID;

public class TestSubClass {

	@ID
	String id;

	String name;

	TheSubClass theSubClass;

	public static class TheSubClass {

		int i;

	}

}
