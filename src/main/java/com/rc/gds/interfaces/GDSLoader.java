package com.rc.gds.interfaces;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import com.rc.gds.GDSLink;
import com.rc.gds.Key;
import com.rc.gds.PropertyContainer;

public interface GDSLoader {
	
	/**
	 * This will fetch a pojo of type clazz with specific id. This method is prefered over fetch(Key)
	 * 
	 * @param clazz
	 *            Class of the object to fetch. Should be the class itself if possible, but superclass or subclass of the object will work
	 *            too.
	 * @param id
	 *            Long id returned from a previous saved pojo
	 * @return
	 */
	public abstract <T> GDSResult<T> fetch(Class<T> clazz, String id);
	
	/**
	 * Will fetch the pojo matching the key.
	 * 
	 * @param key
	 * @return
	 */
	public abstract void fetch(Key key, GDSCallback<Object> callback);
	
	/**
	 * Will fetch all pojos for keys.
	 * 
	 * @param keys
	 * @return
	 * @throws ExecutionException
	 * @throws InterruptedException
	 * @throws IllegalAccessException
	 * @throws InstantiationException
	 * @throws ClassNotFoundException
	 */
	public abstract void fetchBatch(Iterable<Key> keys, GDSCallback<Map<Key, Object>> callback) throws InterruptedException, ExecutionException, ClassNotFoundException,
			InstantiationException, IllegalAccessException;
	
	public abstract void fetchLinks(List<GDSLink> linksToFetch, GDSCallback<List<GDSLink>> callback) throws IllegalArgumentException, IllegalAccessException,
			ClassNotFoundException,
			InstantiationException, InterruptedException, ExecutionException;
	
	/**
	 * Real logic for loading pojos from the datastore. Can be given a Entity or EmbeddedEntity, and if the entity is in the correct format
	 * you will get back a POJO.
	 * 
	 * @param entity
	 * @param id
	 * @return
	 * @throws ClassNotFoundException
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 * @throws InterruptedException
	 * @throws ExecutionException
	 */
	public abstract void entityToPOJO(PropertyContainer entity, String id, List<GDSLink> linksToFetch, GDSCallback<Object> callback) throws ClassNotFoundException,
			InstantiationException,
			IllegalAccessException,
			InterruptedException,
			ExecutionException;
	
}
