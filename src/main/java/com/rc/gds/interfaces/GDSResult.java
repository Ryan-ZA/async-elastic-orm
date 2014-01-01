package com.rc.gds.interfaces;

public interface GDSResult<T> {
	
	public void later(GDSCallback<T> inCallback);
	
	public T now();
	
}
