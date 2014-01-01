package com.rc.gds.interfaces;

public interface GDSCallback<T> {
	public void onSuccess(T t, Throwable err);
}