package org.xson.tangyuan;

import java.util.Map;

import org.xson.tangyuan.datasource.MongoDataSourceManager;
import org.xson.tangyuan.sharding.ShardingDefVo;
import org.xson.tangyuan.xml.XmlMongoConfigBuilder;

import com.mongodb.WriteConcern;

public class TangYuanMongoContainer {

	private static TangYuanMongoContainer	instance			= new TangYuanMongoContainer();
	private MongoDataSourceManager			dataSourceManager	= null;
	private Map<String, ShardingDefVo>		shardingDefMap		= null;
	private int								defaultFetchSize	= 100;

	// 以后考虑放在每个DS中
	private WriteConcern					defaultWriteConcern	= WriteConcern.ACKNOWLEDGED;

	static {
		TangYuanContainer.getInstance().getBuilderMap().put("mongo", new XmlMongoConfigBuilder());
	}

	private TangYuanMongoContainer() {
	}

	public static TangYuanMongoContainer getInstance() {
		return instance;
	}

	public MongoDataSourceManager getDataSourceManager() {
		return dataSourceManager;
	}

	public void setDataSourceManager(MongoDataSourceManager dataSourceManager) {
		this.dataSourceManager = dataSourceManager;
	}

	public ShardingDefVo getShardingDef(String key) {
		return shardingDefMap.get(key);
	}

	public void setShardingDefMap(Map<String, ShardingDefVo> shardingDefMap) {
		this.shardingDefMap = shardingDefMap;
	}

	public int getDefaultFetchSize() {
		return defaultFetchSize;
	}

	public WriteConcern getDefaultWriteConcern() {
		return defaultWriteConcern;
	}

	public void setDefaultWriteConcern(WriteConcern defaultWriteConcern) {
		this.defaultWriteConcern = defaultWriteConcern;
	}

}
