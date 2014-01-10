package com.rc.gds;

import com.rc.gds.annotation.AlwaysPersist;
import com.rc.gds.annotation.ID;

@AlwaysPersist
public class TestChild {

	@ID
	String id;
	
	String name;
	
	int i = 1;
	short j = 1;
	float x = 1;
	byte b = 1;
	double d = 1;
	long l = 1;
	
	Integer ii = 1;
	Short jj = 1;
	Float ff = 1.0f;
	Double dd = 1.0;
	Byte bb = 2;
	Long ll = 1L;

}
