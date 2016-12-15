package org.xson.tangyuan.xml.node;

import org.xson.tangyuan.cache.vo.CacheUseVo;
import org.xson.tangyuan.executor.MongoServiceContext;
import org.xson.tangyuan.executor.ServiceContext;
import org.xson.tangyuan.logging.Log;
import org.xson.tangyuan.logging.LogFactory;

public class MongoSelectVarNode extends AbstractMongoNode {

	private static Log	log	= LogFactory.getLog(MongoSelectVarNode.class);

	private CacheUseVo	cacheUse;

	public MongoSelectVarNode(String id, String ns, String serviceKey, String dsKey, TangYuanNode sqlNode, CacheUseVo cacheUse) {

		this.id = id;
		this.ns = ns;
		this.serviceKey = serviceKey;

		this.dsKey = dsKey;
		this.sqlNode = sqlNode;

		this.cacheUse = cacheUse;

		this.simple = true;
	}

	@Override
	public boolean execute(ServiceContext context, Object arg) throws Throwable {

		MongoServiceContext mongoContext = (MongoServiceContext) context.getServiceContext(TangYuanServiceType.MONGO);

		// 1. cache使用
		if (null != cacheUse) {
			Object result = cacheUse.getObject(arg);
			if (null != result) {
				context.setResult(result);
				return true;
			}
		}

		// 2. 清理和重置执行环境
		mongoContext.resetExecEnv();
		long startTime = System.currentTimeMillis();
		Object result = null;

		sqlNode.execute(context, arg); // 获取sql
		if (log.isInfoEnabled()) {
			log.info(mongoContext.getSql());
		}

		result = mongoContext.executeSelectVar(this);
		context.setResult(result);

		if (log.isInfoEnabled()) {
			log.info("mongo execution time: " + getSlowServiceLog(startTime));
		}

		if (null != cacheUse) {
			cacheUse.putObject(arg, result);
		}

		return true;
	}

}
