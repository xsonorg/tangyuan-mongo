package org.xson.tangyuan.datasource;

import java.util.Map;

public class MongoDataSourceVo {

	private String				id;
	private Map<String, String>	properties;
	private boolean				defaultDs	= false;
	protected boolean			group		= false;

	public MongoDataSourceVo(String id, Map<String, String> properties, boolean defaultDs) {
		this.id = id;
		this.properties = properties;
		this.defaultDs = defaultDs;
	}

	public String getId() {
		return id;
	}

	public Map<String, String> getProperties() {
		return properties;
	}

	public boolean isDefaultDs() {
		return defaultDs;
	}
	
	public boolean isGroup() {
		return group;
	}

}
