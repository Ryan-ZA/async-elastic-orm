package com.rc.gds;

import java.util.Collection;

class GDSLink {

	Object pojo;
	Collection<Object> collection;
	GDSField gdsField;
	Key key;
	Collection<Key> keyCollection;

	public GDSLink(Object pojo, GDSField gdsField, Key key) {
		this.pojo = pojo;
		this.gdsField = gdsField;
		this.key = key;
	}

	public GDSLink(Object pojo, GDSField gdsField, Collection<Key> keyIterable) {
		this.pojo = pojo;
		this.gdsField = gdsField;
		keyCollection = keyIterable;
	}

	public GDSLink(Collection<Object> collection, Collection<Key> keyIterable) {
		this.collection = collection;
		keyCollection = keyIterable;
	}

	public GDSLink(Collection<Object> collection, Key key) {
		this.collection = collection;
		this.key = key;
	}

}
