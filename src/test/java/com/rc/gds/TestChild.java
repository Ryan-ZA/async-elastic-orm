package com.rc.gds;

import com.rc.gds.annotation.AlwaysPersist;
import com.rc.gds.annotation.ID;

@AlwaysPersist
public class TestChild {

	@ID
	String id;
	
	String name;
	
}
