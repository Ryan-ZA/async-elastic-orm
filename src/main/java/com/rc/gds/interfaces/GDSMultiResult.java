package com.rc.gds.interfaces;

import java.util.List;

public interface GDSMultiResult<T> {
	
	public void later(GDSResultReceiver<T> resultReceiver);
	
	public void laterAsList(GDSResultListReceiver<T> resultListReceiver);
	
	public List<T> asList();
	
}
