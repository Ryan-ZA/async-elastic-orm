package com.rc.gds.interfaces;


public interface GDSDeleter {
	
	public abstract GDSResult<Boolean> delete(Object o);
	
	public abstract GDSResult<Boolean> deleteAll(Iterable<?> iterable);
	
}
