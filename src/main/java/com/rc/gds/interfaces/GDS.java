package com.rc.gds.interfaces;

import java.util.List;

import org.elasticsearch.client.Client;

import com.rc.gds.Key;

public interface GDS {
	
	/**
	 * Begin a transaction that will last until commitTransaction() or rollbackTransaction() is called. You must call one of these when you
	 * have finished the transaction. The transaction will apply to all load() save() and query() calls from this GDS.
	 * 
	 * It is not required to call this to do simple operations - you only need to use this if you wish to commit/rollback all operations
	 * done by this GDS.
	 */
	public abstract void beginTransaction();
	
	/**
	 * Must call beginTransaction before using this or you will receive a NullPointerException.
	 */
	public abstract void commitTransaction();
	
	/**
	 * 
	 * @return A new GDSDelete that can be used to delete pojos from the datastore
	 */
	public abstract GDSDeleter delete();
	
	/**
	 * @param <T>
	 * @return Delete a single pojo
	 */
	public abstract <T> GDSResult<Boolean> delete(T t);

	public abstract Client getClient();
	
	public abstract String indexFor(String kind);
	
	public abstract String[] indexFor(String[] kinds);
	
	/**
	 * @return A new GDSLoader that can be used to load pojos IFF you have the ID or Key of the pojo.
	 */
	public abstract GDSLoader load();
	
	/**
	 * A way to fetch an object if you don't know the class - only a stored class string
	 * 
	 * @return
	 */
	public abstract GDSResult<Object> load(String kind, String id);
	
	/**
	 * Fetch a single pojo
	 * 
	 * @param <T>
	 * 
	 * @param ownerKind
	 * @param ownerID
	 * @return
	 */
	public abstract <T> GDSResult<T> load(Class<T> clazz, String id);
	
	/**
	 * @param clazz
	 *            The class of pojos to search for. All subclasses of this type will also be searched for.
	 * @return A new parametrized GDSQuery that can be used to search for specific kinds of pojos. Filters and sorting are available.
	 */
	public abstract <T> GDSQuery<T> query(Class<T> clazz);
	
	/**
	 * Must call beginTransaction before using this or you will receive a NullPointerException.
	 */
	public abstract void rollbackTransaction();
	
	/**
	 * @return A new GDSSaver that can be used to save any collection of pojos.
	 */
	public abstract GDSSaver save();

	/**
	 * @param <T>
	 * @return Save a single pojo
	 */
	public abstract <T> GDSResult<Key> save(T t);
	
	public abstract <T> GDSResult<List<T>> fetchAll(Class<T> clazz, String[] ids);
	
	public abstract <T> GDSMultiResult<T> fetchAll(Class<T> clazz);
	
}
