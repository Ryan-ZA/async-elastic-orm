package com.rc.gds.interfaces;

public interface GDSResultReceiver<T> {
	
	/**
	 * Called with a new pojo result
	 * 
	 * @param t
	 * @return true to receive the next result, false to stop receiving results
	 */
	public boolean receiveNext(T t);
	
	/**
	 * Called when results have reached the end or when receiveNext returns false
	 */
	public void finished();
	
	/**
	 * Called if an error occurs at any point during the query or parsing the results
	 * 
	 * @param err
	 */
	public void onError(Throwable err);

}
