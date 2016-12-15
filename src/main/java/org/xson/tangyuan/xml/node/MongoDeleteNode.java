package org.xson.tangyuan.xml.node;

import org.xson.tangyuan.cache.vo.CacheCleanVo;
import org.xson.tangyuan.executor.MongoServiceContext;
import org.xson.tangyuan.executor.ServiceContext;
import org.xson.tangyuan.logging.Log;
import org.xson.tangyuan.logging.LogFactory;

public class MongoDeleteNode extends AbstractMongoNode {

	private static Log		log	= LogFactory.getLog(MongoDeleteNode.class);

	private CacheCleanVo	cacheClean;

	public MongoDeleteNode(String id, String ns, String serviceKey, String dsKey, TangYuanNode sqlNode, CacheCleanVo cacheClean) {
		this.id = id;
		this.ns = ns;
		this.serviceKey = serviceKey;
		this.dsKey = dsKey;
		this.sqlNode = sqlNode;
		this.simple = true;
		this.cacheClean = cacheClean;
	}

	@Override
	public boolean execute(ServiceContext context, Object arg) throws Throwable {
		MongoServiceContext mongoContext = (MongoServiceContext) context.getServiceContext(TangYuanServiceType.MONGO);

		// 2. 清理和重置执行环境
		mongoContext.resetExecEnv();

		long startTime = System.currentTimeMillis();
		sqlNode.execute(context, arg); // 获取sql
		if (log.isInfoEnabled()) {
			log.info(mongoContext.getSql());
		}

		int result = mongoContext.executeDelete(this);
		context.setResult(result);

		if (log.isInfoEnabled()) {
			log.info("mongo execution time: " + getSlowServiceLog(startTime));
		}

		if (null != cacheClean) {
			cacheClean.removeObject(arg);
		}

		return true;
	}

}
