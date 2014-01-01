package com.rc.gds;

import java.util.Map;

interface PropertyContainer {

	public void setProperty(String key, Object val);

	public void setUnindexedProperty(String key, Object val);
	
	public Map<String, Object> getDBDbObject();

	public Object getProperty(String key);
	
	public boolean hasProperty(String key);
	
}
