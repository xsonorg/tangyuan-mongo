package org.xson.tangyuan.datasource;

import com.mongodb.DBCollection;

public class SimpleMongoDataSourceManager implements MongoDataSourceManager {

	private MongoDataSource	dataSource		= null;

	private String			dataSourceId	= null;

	public SimpleMongoDataSourceManager(MongoDataSource dataSource, String dataSourceId) {
		this.dataSource = dataSource;
		this.dataSourceId = dataSourceId;
	}

	@Override
	public boolean isValidDsKey(String dsKey) {
		return dsKey.equals(this.dataSourceId);
	}

	@Override
	public DBCollection getCollection(String dsKey, String collection) {
		return dataSource.getCollection(collection);
	}

	@Override
	public void close() {
		dataSource.close();
	}

}
