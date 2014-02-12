package com.rc.gds.interfaces;

import java.util.List;

import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.sort.SortBuilder;

import com.rc.gds.GDSQueryResultImpl;

public interface GDSQuery<T> {
	
	/**
	 * Add a filter to this query. Read GAE documentation on datastore queries for more information.
	 * 
	 * @param filter
	 * @return
	 */
	public abstract GDSQuery<T> filter(QueryBuilder query);
	
	/**
	 * 
	 * Create a datastore filter that will match a field equal to a pojo. This filter will then filter out all entities that do not have the
	 * specified field set to the pojo. Pojo matches are done on pojo type + id - none of the other member fields count towards the match.
	 * 
	 * @param field
	 * @param pojo
	 * @return this (for chaining)
	 */
	public abstract GDSQuery<T> filter(String field, Object pojo);
	
	/**
	 * Adds a sort to this query. Check out datastore documentation on sorting.
	 * 
	 * @param field
	 * @param direction
	 * @return this (for chaining)
	 */
	public abstract GDSQuery<T> sort(SortBuilder sortBuilder);
	
	/**
	 * 
	 * @return this (for chaining)
	 */
	public abstract GDSQuery<T> keysOnly();
	
	/**
	 * Can be set with a cursor to continue a previous query. Can be passed a null cursor, will be a NO-OP.
	 * 
	 * @param cursor
	 *            A cursor string from a previous query. Should be safe to pass to clients as it cannot be modified, but it is possible to
	 *            see class names and similar in the query (but no actual data). You should encrypt this cursor if this is a problem.
	 * @return this (for chaining)
	 */
	public abstract GDSQuery<T> continueFrom(String cursor);
	
	public abstract GDSQueryResultImpl<T> result();
	
	public abstract List<T> asList();
	
	public abstract GDSQuery<T> size(int i);
	
	public abstract GDSQuery<T> skip(int i);
	
}
