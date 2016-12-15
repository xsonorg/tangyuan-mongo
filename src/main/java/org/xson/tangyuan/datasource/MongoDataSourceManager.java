package org.xson.tangyuan.datasource;

import com.mongodb.DBCollection;

public interface MongoDataSourceManager {

	public boolean isValidDsKey(String dsKey);

	public DBCollection getCollection(String dsKey, String collection);

	public void close();
}
