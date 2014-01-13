package com.rc.gds;

import java.util.List;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.sort.SortBuilder;

public class GDSQuery<T> {

	GDS gds;
	Class<T> clazz;
	FilterBuilder filter;
	BoolQueryBuilder boolquery;
	SortBuilder sort;
	String collectionName;

	int skip = 0;
	int size = 1000;

	protected GDSQuery(GDS gds, Class<T> clazz) {
		this.gds = gds;
		this.clazz = clazz;
		
		collectionName = GDSClass.fixName(GDSClass.getBaseClass(clazz).getName());

		filter = FilterBuilders.andFilter(
				FilterBuilders.typeFilter(collectionName),
				FilterBuilders.queryFilter(QueryBuilders.fieldQuery(GDSClass.GDS_FILTERCLASS_FIELD, GDSClass.fixName(clazz.getName()))));
	}
	
	/**
	 * Add a filter to this query. Read GAE documentation on datastore queries for more information.
	 * 
	 * @param filter
	 * @return
	 */
	public GDSQuery<T> filter(QueryBuilder query) {
		if (boolquery == null)
			boolquery = QueryBuilders.boolQuery();
		
		boolquery.must(query);
		return this;
	}
	
	/**
	 * Create a datastore filter that will match a field equal to a pojo. This filter will then filter out all entities that do not have the
	 * specified field set to the pojo. Pojo matches are done on pojo type + id - none of the other member fields count towards the match.
	 * 
	 * @param field
	 * @param operator
	 *            MongoDB query operator. Use null for "equals"
	 * @param pojo
	 * @return A filter object that can be passed to filter method or used in CompositeFilters
	 */
	public static QueryBuilder createPojoFilter(String field, String operator, Object pojo) {
		try {
			GDSField idField = GDSField.createMapFromObject(pojo).get(GDSField.GDS_ID_FIELD);
			if (idField == null)
				throw new RuntimeException("Class " + pojo.getClass().getName() + " does not have an ID field!");
			
			// Get the ID and create the low level entity
			String id = (String) GDSField.getValue(idField, pojo);
			
			return QueryBuilders.fieldQuery(field + ".id", id);
		} catch (Exception ex) {
			throw new RuntimeException(ex);
		}
	}
	
	/**
	 * 
	 * Create a datastore filter that will match a field equal to a pojo. This filter will then filter out all entities that do not have the
	 * specified field set to the pojo. Pojo matches are done on pojo type + id - none of the other member fields count towards the match.
	 * 
	 * @param field
	 * @param pojo
	 * @return this (for chaining)
	 */
	public GDSQuery<T> filter(String field, Object pojo) {
		filter(createPojoFilter(field, null, pojo));
		return this;
	}
	
	/**
	 * Adds a sort to this query. Check out datastore documentation on sorting.
	 * 
	 * @param field
	 * @param direction
	 * @return this (for chaining)
	 */
	public GDSQuery<T> sort(SortBuilder sortBuilder) {
		sort = sortBuilder;
		return this;
	}
	
	/**
	 * 
	 * @return this (for chaining)
	 */
	public GDSQuery<T> keysOnly() {
		// Null op
		return this;
	}
	
	/**
	 * Can be set with a cursor to continue a previous query. Can be passed a null cursor, will be a NO-OP.
	 * 
	 * @param cursor
	 *            A cursor string from a previous query. Should be safe to pass to clients as it cannot be modified, but it is possible to
	 *            see class names and similar in the query (but no actual data). You should encrypt this cursor if this is a problem.
	 * @return this (for chaining)
	 */
	public GDSQuery<T> continueFrom(String cursor) {
		if (cursor != null)
			skip = Integer.valueOf(cursor);
		return this;
	}
	
	public GDSQueryResultImpl<T> result() {
		SearchRequestBuilder requestBuilder = gds.client.prepareSearch(gds.indexFor(collectionName));
		if (sort != null)
			requestBuilder.addSort(sort);
		
		requestBuilder.setQuery(boolquery == null ? QueryBuilders.matchAllQuery() : boolquery);
		if (filter != null)
			requestBuilder.setFilter(filter);
		
		requestBuilder.setFrom(0);
		requestBuilder.setSize(10000);

		SearchResponse searchResponse = requestBuilder.execute().actionGet();
		
		return new GDSQueryResultImpl<T>(gds, clazz, searchResponse.getHits().iterator(), searchResponse);
	}
	
	public List<T> asList() {
		return result().asList();
	}

	public GDSQuery<T> size(int i) {
		if (i > 1000)
			throw new RuntimeException("Size cannot be bigger than 1000 (temporary limit)");
		size = i;
		return this;
	}
	
	public GDSQuery<T> skip(int i) {
		skip = i;
		return this;
	}

}
