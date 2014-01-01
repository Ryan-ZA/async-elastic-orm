package com.rc.gds;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import com.rc.gds.interfaces.GDSCallback;
import com.rc.gds.interfaces.GDSMultiResult;
import com.rc.gds.interfaces.GDSResult;
import com.rc.gds.interfaces.GDSResultListReceiver;

public class GDSBatcher {
	
	final GDSResult<?>[] singleResults;
	final GDSMultiResult<?>[] multiResults;
	
	public GDSBatcher(GDSResult<?>... input) {
		singleResults = input;
		multiResults = null;
	}
	
	public GDSBatcher(GDSMultiResult<?>... input) {
		singleResults = null;
		multiResults = input;
	}
	
	public GDSBatcher(GDSResult<?>[] singleResults, GDSMultiResult<?>[] multiResults) {
		this.singleResults = singleResults;
		this.multiResults = multiResults;
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public GDSResult<Boolean> onAllComplete() {
		final AtomicInteger current = new AtomicInteger(0);
		final AtomicInteger total = new AtomicInteger(0);
		final GDSAsyncImpl<Boolean> asyncResult = new GDSAsyncImpl<>();
		
		final GDSCallback singleCallback = new GDSCallback() {
			
			@Override
			public void onSuccess(Object t, Throwable err) {
				handleSuccess(current, total, asyncResult, err);
			}
		};
		final GDSResultListReceiver multiCallback = new GDSResultListReceiver() {
			
			@Override
			public void success(List list, Throwable err) {
				handleSuccess(current, total, asyncResult, err);
			}
		};

		if (singleResults != null) {
			total.addAndGet(singleResults.length);
			for (GDSResult<?> result : singleResults)
				result.later(singleCallback);
		}
		if (multiResults != null) {
			total.addAndGet(multiResults.length);
			for (GDSMultiResult<?> result : multiResults)
				result.laterAsList(multiCallback);
		}
		
		return asyncResult;
	}

	protected void handleSuccess(AtomicInteger current, AtomicInteger total, GDSAsyncImpl<Boolean> result, Throwable err) {
		if (err != null) {
			result.onSuccess(false, err);
		} else if (current.incrementAndGet() == total.get()) {
			result.onSuccess(true, err);
		}
	}

}
