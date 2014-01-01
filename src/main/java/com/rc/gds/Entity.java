package com.rc.gds;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

class Entity implements PropertyContainer, Serializable {

	private static final long serialVersionUID = 1L;

	String id;
	String classKind;
	Long version;
	Map<String, Object> dbObject = new HashMap<>();

	public Entity() {
	}

	public Entity(String classKind, String specialID) {
		this.classKind = classKind;
		id = specialID;
	}

	public Entity(String classKind) {
		this.classKind = classKind;
	}

	public Entity(String classKind, Map<String, Object> dbObject) {
		this.classKind = classKind;
		this.dbObject = dbObject;
	}

	@Override
	public void setProperty(String key, Object val) {
		if (val instanceof Key) {
			dbObject.put(key, ((Key) val).toMap());
		} else if (val instanceof EmbeddedEntity) {
			dbObject.put(key, ((EmbeddedEntity) val).getDBDbObject());
		} else {
			dbObject.put(key, val);
		}
	}

	@Override
	public void setUnindexedProperty(String key, Object val) {
		setProperty(key, val);
	}

	public String getKind() {
		return classKind;
	}

	@Override
	public Map<String, Object> getDBDbObject() {
		return dbObject;
	}

	public Key getKey() {
		return new Key(classKind, id);
	}

	@Override
	public Object getProperty(String key) {
		return dbObject.get(key);
	}

	@Override
	public boolean hasProperty(String key) {
		return dbObject.containsKey(key);
	}

	public Long getVersion() {
		return version;
	}
	
	public void setVersion(Long version) {
		this.version = version;
	}

}
