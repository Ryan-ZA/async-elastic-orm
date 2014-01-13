package com.rc.gds;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.SearchHit;

import com.rc.gds.interfaces.GDSCallback;
import com.rc.gds.interfaces.GDSMultiResult;
import com.rc.gds.interfaces.GDSResultListReceiver;
import com.rc.gds.interfaces.GDSResultReceiver;

public class GDSQueryResultImpl<T> implements GDSMultiResult<T> {
	
	private static final int MAX_DEPTH = 100;

	GDS gds;
	Iterator<SearchHit> iterator;
	Class<T> clazz;
	GDSLoader loader;
	SearchResponse searchResponse;
	private boolean finished = false;
	int depth = 0;
	ExecutorService deepStackExecutor;
	
	protected GDSQueryResultImpl(GDS gds, Class<T> clazz, Iterator<SearchHit> iterator, SearchResponse searchResponse) {
		this.gds = gds;
		this.iterator = iterator;
		this.clazz = clazz;
		this.searchResponse = searchResponse;
		loader = new GDSLoader(gds);
	}
	
	@Override
	public void later(final GDSResultReceiver<T> resultReceiver) {
		if (!iterator.hasNext()) {
			resultReceiver.finished();
			return;
		}

		SearchHit hit = iterator.next();
		Entity entity = new Entity(hit.getType(), hit.getId(), hit.getSource());
		
		final List<GDSLink> links = new ArrayList<GDSLink>();
		try {
			loader.entityToPOJO(entity, hit.getId(), links, new GDSCallback<Object>() {
				
				@Override
				public void onSuccess(final Object pojo, Throwable err) {
					try {
						if (err != null)
							throw err;
						loader.fetchLinks(links, new GDSCallback<List<GDSLink>>() {
							
							@SuppressWarnings("unchecked")
							@Override
							public void onSuccess(List<GDSLink> t, Throwable err) {
								if (err != null) {
									shutdownDeepStackExecutor();
									resultReceiver.onError(err);
								} else {
									if (resultReceiver.receiveNext((T) pojo)) {
										depth++;
										if (depth > MAX_DEPTH) {
											// Temporary work around for depth getting too great on the stack
											if (deepStackExecutor == null)
												deepStackExecutor = Executors.newSingleThreadExecutor();
											deepStackExecutor.submit(new Runnable() {

												@Override
												public void run() {
													later(resultReceiver);
												}
											});
										} else {
											later(resultReceiver);
										}
									} else {
										shutdownDeepStackExecutor();
										resultReceiver.finished();
									}
								}
							}
						});
					} catch (Throwable e) {
						e.printStackTrace();
						resultReceiver.onError(e);
					}
				}
			});
		} catch (ClassNotFoundException | InstantiationException | IllegalAccessException | InterruptedException | ExecutionException e) {
			e.printStackTrace();
			throw new RuntimeException("Error completing query on " + clazz, e);
		}
	}
	
	@Override
	public List<T> asList() {
		finished = false;
		final List<T> result = new ArrayList<>();
		final List<Throwable> errList = new LinkedList<>();
		
		later(new GDSResultReceiver<T>() {
			
			@Override
			public boolean receiveNext(T t) {
				synchronized (result) {
					result.add(t);
				}
				return true;
			}
			
			@Override
			public void finished() {
				synchronized (result) {
					finished = true;
					result.notifyAll();
				}
			}
			
			@Override
			public void onError(Throwable err) {
				synchronized (result) {
					errList.add(err);
					result.notifyAll();
				}
			}
		});
		
		synchronized (result) {
			try {
				if (!errList.isEmpty())
					throw new RuntimeException("Error completing query on " + clazz, errList.get(0));
				if (finished)
					return result;
				result.wait();
				if (!errList.isEmpty())
					throw new RuntimeException("Error completing query on " + clazz, errList.get(0));
				return result;
			} catch (InterruptedException e) {
				e.printStackTrace();
				throw new RuntimeException("Error completing query on " + clazz, e);
			}
		}
	}
	
	@Override
	public void laterAsList(final GDSResultListReceiver<T> resultListReceiver) {
		later(new GDSResultReceiver<T>() {
			
			List<T> list = new ArrayList<>();
			
			@Override
			public boolean receiveNext(T t) {
				list.add(t);
				return true;
			}
			
			@Override
			public void finished() {
				resultListReceiver.success(list, null);
			}
			
			@Override
			public void onError(Throwable err) {
				resultListReceiver.success(null, err);
			}
		});
	}
	
	private void shutdownDeepStackExecutor() {
		if (deepStackExecutor != null)
			deepStackExecutor.shutdown();
	}

}
