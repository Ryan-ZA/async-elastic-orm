package com.rc.gds.interfaces;

import java.util.List;

public interface GDSResultListReceiver<T> {
	
	public void success(List<T> list, Throwable err);

}
