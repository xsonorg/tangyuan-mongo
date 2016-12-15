package org.xson.tangyuan.datasource;

import java.util.Map;

import com.mongodb.DBCollection;

public class MuiltMongoDataSourceManager implements MongoDataSourceManager {

	/**
	 * 逻辑上的
	 */
	protected Map<String, MongoDataSourceVo>	logicDataSourceMap	= null;

	/**
	 * 所有的
	 */
	protected Map<String, MongoDataSource>		realDataSourceMap	= null;

	public MuiltMongoDataSourceManager(Map<String, MongoDataSourceVo> logicDataSourceMap, Map<String, MongoDataSource> realDataSourceMap) {
		this.logicDataSourceMap = logicDataSourceMap;
		this.realDataSourceMap = realDataSourceMap;
	}

	@Override
	public boolean isValidDsKey(String dsKey) {
		if (dsKey.indexOf(".") < 0) {
			return null != logicDataSourceMap.get(dsKey);
		}
		return null != realDataSourceMap.get(dsKey);
	}

	@Override
	public DBCollection getCollection(String dsKey, String collection) {
		MongoDataSource dataSource = realDataSourceMap.get(dsKey);
		if (null == dataSource) {
			throw new DataSourceException("A non-existent mongo data source: " + dsKey);
		}
		return dataSource.getCollection(collection);
	}

	@Override
	public void close() {
		for (Map.Entry<String, MongoDataSource> entry : realDataSourceMap.entrySet()) {
			entry.getValue().close();
		}
	}

}
