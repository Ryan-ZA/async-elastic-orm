package com.rc.gds;

import java.util.List;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.sort.SortBuilder;

import com.rc.gds.interfaces.GDS;
import com.rc.gds.interfaces.GDSQuery;

public class GDSQueryImpl<T> implements GDSQuery<T> {

	GDS gds;
	Class<T> clazz;
	BoolQueryBuilder boolquery;
	SortBuilder sort;
	String collectionName;

	int skip = 0;
	int size = 1000;

	protected GDSQueryImpl(GDS gds, Class<T> clazz) {
		this.gds = gds;
		this.clazz = clazz;
		
		collectionName = GDSClass.getKind(clazz);
		
		boolquery = QueryBuilders.boolQuery();
		boolquery.must(QueryBuilders.matchPhraseQuery(GDSClass.GDS_FILTERCLASS_FIELD, GDSClass.fixName(clazz.getName())));
	}
	
	/*
	 * (non-Javadoc)
	 * @see com.rc.gds.GDSQuery#filter(org.elasticsearch.index.query.QueryBuilder)
	 */
	@Override
	public GDSQuery<T> filter(QueryBuilder query) {
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
			
			return QueryBuilders.matchPhraseQuery(field + ".id", id);
		} catch (Exception ex) {
			throw new RuntimeException(ex);
		}
	}
	
	/*
	 * (non-Javadoc)
	 * @see com.rc.gds.GDSQuery#filter(java.lang.String, java.lang.Object)
	 */
	@Override
	public GDSQuery<T> filter(String field, Object pojo) {
		filter(createPojoFilter(field, null, pojo));
		return this;
	}
	
	/*
	 * (non-Javadoc)
	 * @see com.rc.gds.GDSQuery#sort(org.elasticsearch.search.sort.SortBuilder)
	 */
	@Override
	public GDSQuery<T> sort(SortBuilder sortBuilder) {
		sort = sortBuilder;
		return this;
	}
	
	/*
	 * (non-Javadoc)
	 * @see com.rc.gds.GDSQuery#keysOnly()
	 */
	@Override
	public GDSQuery<T> keysOnly() {
		// Null op
		return this;
	}
	
	/*
	 * (non-Javadoc)
	 * @see com.rc.gds.GDSQuery#continueFrom(java.lang.String)
	 */
	@Override
	public GDSQuery<T> continueFrom(String cursor) {
		if (cursor != null)
			skip = Integer.valueOf(cursor);
		return this;
	}
	
	/*
	 * (non-Javadoc)
	 * @see com.rc.gds.GDSQuery#result()
	 */
	@Override
	public GDSQueryResultImpl<T> result() {
		SearchRequestBuilder requestBuilder = gds.getClient().prepareSearch(gds.indexFor(collectionName));
		requestBuilder.setTypes(collectionName);
		if (sort != null)
			requestBuilder.addSort(sort);
		
		requestBuilder.setQuery(boolquery);
		
		requestBuilder.setFrom(0);
		requestBuilder.setSize(10000);

		SearchResponse searchResponse = requestBuilder.execute().actionGet();
		
		return new GDSQueryResultImpl<T>(gds, clazz, searchResponse.getHits().iterator(), searchResponse);
	}
	
	/*
	 * (non-Javadoc)
	 * @see com.rc.gds.GDSQuery#asList()
	 */
	@Override
	public List<T> asList() {
		return result().asList();
	}
	
	/*
	 * (non-Javadoc)
	 * @see com.rc.gds.GDSQuery#size(int)
	 */
	@Override
	public GDSQuery<T> size(int i) {
		if (i > 1000)
			throw new RuntimeException("Size cannot be bigger than 1000 (temporary limit)");
		size = i;
		return this;
	}
	
	/*
	 * (non-Javadoc)
	 * @see com.rc.gds.GDSQuery#skip(int)
	 */
	@Override
	public GDSQuery<T> skip(int i) {
		skip = i;
		return this;
	}

}
