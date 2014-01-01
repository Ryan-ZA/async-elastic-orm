package com.rc.gds;

import com.rc.gds.annotation.ID;
import com.rc.gds.annotation.Version;

public class TestVersionedObject {
	@ID
	String id;
	@Version
	long ver;
	
	String name;
}
