package com.rc.gds;

import java.lang.reflect.InvocationTargetException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.deletebyquery.DeleteByQueryResponse;
import org.elasticsearch.index.query.QueryBuilders;

import com.rc.gds.interfaces.GDS;
import com.rc.gds.interfaces.GDSDeleter;
import com.rc.gds.interfaces.GDSResult;

public class GDSDeleterImpl implements GDSDeleter {
	
	GDS gds;
	
	GDSDeleterImpl(GDS gds) {
		this.gds = gds;
	}
	
	/*
	 * (non-Javadoc)
	 * @see com.rc.gds.GDSDeleter#delete(java.lang.Object)
	 */
	@Override
	public GDSResult<Boolean> delete(Object o) {
		try {
			GDSClass.onPreDelete(gds, o);
			String id = GDSField.getID(o);
			return delete(o.getClass(), id);
		} catch (IllegalArgumentException | IllegalAccessException | InvocationTargetException e) {
			throw new RuntimeException("Reflection error", e);
		}
	}

	protected GDSResult<Boolean> delete(Class<?> clazz, String id) {
		if (id == null)
			throw new NullPointerException("delete id cannot be null");
		
		final GDSAsyncImpl<Boolean> callback = new GDSAsyncImpl<>();

		String kind = GDSClass.getKind(clazz);
		gds.getClient().prepareDelete(gds.indexFor(kind), kind, id).execute(new ActionListener<DeleteResponse>() {
			
			@Override
			public void onResponse(DeleteResponse response) {
				callback.onSuccess(response.isFound(), null);
			}
			
			@Override
			public void onFailure(Throwable e) {
				callback.onSuccess(false, e);
			}
		});
		
		return callback;
	}
	
	/*
	 * (non-Javadoc)
	 * @see com.rc.gds.GDSDeleter#deleteAll(java.lang.Iterable)
	 */
	@Override
	public GDSResult<Boolean> deleteAll(Iterable<?> iterable) {
		try {
			final GDSAsyncImpl<Boolean> callback = new GDSAsyncImpl<>();
			
			Set<String> ids = new HashSet<>();
			Set<String> kinds = new HashSet<>();
			
			for (Object o : iterable) {
				GDSClass.onPreDelete(gds, o);
				String id = GDSField.getID(o);
				String kind = GDSClass.getKind(o);
				
				ids.add(id);
				kinds.add(kind);
			}
			
			String[] kindsArr = kinds.toArray(new String[kinds.size()]);
			String[] idArr = ids.toArray(new String[ids.size()]);
			
			gds.getClient().prepareDeleteByQuery(gds.indexFor(kindsArr))
					.setQuery(QueryBuilders.idsQuery(kindsArr).ids(idArr)).execute(new ActionListener<DeleteByQueryResponse>() {
						
						@Override
						public void onResponse(DeleteByQueryResponse response) {
							callback.onSuccess(true, null);
						}
						
						@Override
						public void onFailure(Throwable e) {
							callback.onSuccess(false, e);
						}
					});
			
			return callback;
		} catch (IllegalArgumentException | IllegalAccessException | InvocationTargetException e) {
			throw new RuntimeException("Reflection error", e);
		}
	}
	
	protected GDSResult<Boolean> deleteAll(Class<?> clazz, List<String> ids) {
		
		final GDSAsyncImpl<Boolean> callback = new GDSAsyncImpl<>();
		
		String kind = GDSClass.getKind(clazz);
		String[] idArr = ids.toArray(new String[ids.size()]);
		gds.getClient().prepareDeleteByQuery(gds.indexFor(kind))
				.setQuery(QueryBuilders.idsQuery(kind).ids(idArr)).execute(new ActionListener<DeleteByQueryResponse>() {
					
					@Override
					public void onResponse(DeleteByQueryResponse response) {
						callback.onSuccess(true, null);
					}
					
					@Override
					public void onFailure(Throwable e) {
						callback.onSuccess(false, e);
					}
				});
		
		return callback;
	}

}
