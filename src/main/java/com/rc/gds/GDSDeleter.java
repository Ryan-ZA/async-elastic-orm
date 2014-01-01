package com.rc.gds;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.deletebyquery.DeleteByQueryResponse;
import org.elasticsearch.index.query.QueryBuilders;

import com.rc.gds.interfaces.GDSResult;

public class GDSDeleter {
	
	GDS gds;
	
	GDSDeleter(GDS gds) {
		this.gds = gds;
	}
	
	public GDSResult<Boolean> delete(Object o) {
		try {
			String id = GDSField.getID(o);
			return delete(o.getClass(), id);
		} catch (IllegalArgumentException | IllegalAccessException e) {
			e.printStackTrace();
			throw new RuntimeException("Reflection error", e);
		}
	}

	public GDSResult<Boolean> delete(Class<?> clazz, String id) {
		if (id == null)
			throw new NullPointerException("delete id cannot be null");
		
		final GDSAsyncImpl<Boolean> callback = new GDSAsyncImpl<>();

		String kind = GDSClass.fixName(GDSClass.getBaseClass(clazz).getName());
		gds.client.prepareDelete(gds.indexFor(kind), kind, id).execute(new ActionListener<DeleteResponse>() {
			
			@Override
			public void onResponse(DeleteResponse response) {
				callback.onSuccess(!response.isNotFound(), null);
			}
			
			@Override
			public void onFailure(Throwable e) {
				e.printStackTrace();
				callback.onSuccess(false, e);
			}
		});
		
		return callback;
	}
	
	public GDSResult<Boolean> deleteAll(Iterable<?> iterable) {
		try {
			final GDSAsyncImpl<Boolean> callback = new GDSAsyncImpl<>();
			
			Set<String> ids = new HashSet<>();
			Set<String> kinds = new HashSet<>();
			
			for (Object o : iterable) {
				String id = GDSField.getID(o);
				String kind = GDSClass.fixName(GDSClass.getBaseClass(o.getClass()).getName());
				
				ids.add(id);
				kinds.add(kind);
			}
			
			String[] kindsArr = kinds.toArray(new String[kinds.size()]);
			String[] idArr = ids.toArray(new String[ids.size()]);
			
			gds.client.prepareDeleteByQuery(gds.indexFor(kindsArr))
					.setQuery(QueryBuilders.idsQuery(kindsArr).ids(idArr)).execute(new ActionListener<DeleteByQueryResponse>() {
						
						@Override
						public void onResponse(DeleteByQueryResponse response) {
							callback.onSuccess(true, null);
						}
						
						@Override
						public void onFailure(Throwable e) {
							e.printStackTrace();
							callback.onSuccess(false, e);
						}
					});
			
			return callback;
		} catch (IllegalArgumentException | IllegalAccessException e) {
			e.printStackTrace();
			throw new RuntimeException("Reflection error", e);
		}
	}
	
	public GDSResult<Boolean> deleteAll(Class<?> clazz, List<String> ids) {
		
		final GDSAsyncImpl<Boolean> callback = new GDSAsyncImpl<>();
		
		String kind = GDSClass.fixName(GDSClass.getBaseClass(clazz).getName());
		String[] idArr = ids.toArray(new String[ids.size()]);
		gds.client.prepareDeleteByQuery(gds.indexFor(kind))
				.setQuery(QueryBuilders.idsQuery(kind).ids(idArr)).execute(new ActionListener<DeleteByQueryResponse>() {
					
					@Override
					public void onResponse(DeleteByQueryResponse response) {
						callback.onSuccess(true, null);
					}
					
					@Override
					public void onFailure(Throwable e) {
						e.printStackTrace();
						callback.onSuccess(false, e);
					}
				});
		
		return callback;
	}

}
