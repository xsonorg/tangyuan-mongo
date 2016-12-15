package org.xson.tangyuan.datasource;

import com.mongodb.DBCollection;

/**
 * Mongo连接辅助类
 */
public class MongoSupport {

	private static MongoDataSourceManager manager;

	public static void setManager(MongoDataSourceManager manager) {
		if (null == MongoSupport.manager) {
			MongoSupport.manager = manager;
		}
	}

	public static MongoDataSourceManager getManager() {
		return manager;
	}

	public static DBCollection getCollection(String dsKey, String collection) {
		if (null != manager) {
			return manager.getCollection(dsKey, collection);
		}
		return null;
	}

	public static void close() {
		if (null != manager) {
			manager.close();
		}
	}
}
