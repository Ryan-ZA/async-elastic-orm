package com.rc.gds;

import java.util.ArrayList;
import java.util.List;

import com.rc.gds.interfaces.GDSCallback;
import com.rc.gds.interfaces.GDSResult;

public class GDSAsyncImpl<T> implements GDSCallback<T>, GDSResult<T> {
	
	T result = null;
	Throwable resultErr = null;
	List<GDSCallback<T>> callbacks = new ArrayList<>();
	
	public GDSAsyncImpl() {
	}
	
	@Override
	public synchronized void later(GDSCallback<T> inCallback) {
		callbacks.add(inCallback);
		if (result != null || resultErr != null)
			inCallback.onSuccess(result, resultErr);
	}
	
	@Override
	public T now() {
		if (result != null)
			return result;
		
		callbacks.add(new GDSCallback<T>() {
			
			@Override
			public void onSuccess(T t, Throwable err) {
				synchronized (GDSAsyncImpl.this) {
					GDSAsyncImpl.this.notifyAll();
				}
			}
		});
		
		synchronized (GDSAsyncImpl.this) {
			try {
				if (result != null)
					return result;
				GDSAsyncImpl.this.wait();
				return result;
			} catch (InterruptedException e) {
				e.printStackTrace();
				return null;
			}
		}
	}
	
	@Override
	public void onSuccess(T t, Throwable err) {
		result = t;
		resultErr = err;
		for (int i = 0; i < callbacks.size(); i++) {
			GDSCallback<T> callback = callbacks.get(i);
			callback.onSuccess(t, err);
		}
	}
	
}
