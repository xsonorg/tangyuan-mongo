package org.xson.tangyuan.xml.node;

import org.xson.tangyuan.cache.vo.CacheCleanVo;
import org.xson.tangyuan.executor.MongoServiceContext;
import org.xson.tangyuan.executor.ServiceContext;
import org.xson.tangyuan.logging.Log;
import org.xson.tangyuan.logging.LogFactory;
import org.xson.tangyuan.ognl.Ognl;

/**
 * 内部的InsertNode
 */
public class InternalMongoInsertNode extends AbstractMongoNode {

	private static Log		log	= LogFactory.getLog(InternalMongoInsertNode.class);

	// 这里是返回_id
	private String			resultKey;

	// private String incrementKey;

	private CacheCleanVo	cacheClean;

	// public InternalMongoInsertNode(String dsKey, String rowCount, String incrementKey, TangYuanNode sqlNode, CacheCleanVo cacheClean) {
	// this.dsKey = dsKey;
	// this.resultKey = rowCount;
	// this.incrementKey = incrementKey;
	// this.sqlNode = sqlNode;
	// this.simple = false;
	// this.cacheClean = cacheClean;
	// }

	public InternalMongoInsertNode(String dsKey, String resultKey, TangYuanNode sqlNode, CacheCleanVo cacheClean) {
		this.dsKey = dsKey;
		this.resultKey = resultKey;
		// this.incrementKey = incrementKey;
		this.sqlNode = sqlNode;
		this.simple = false;
		this.cacheClean = cacheClean;
	}

	@Override
	public boolean execute(ServiceContext context, Object arg) throws Throwable {

		MongoServiceContext mongoContext = (MongoServiceContext) context.getServiceContext(TangYuanServiceType.MONGO);

		mongoContext.resetExecEnv();

		long startTime = System.currentTimeMillis();
		sqlNode.execute(context, arg); // 获取sql

		if (log.isInfoEnabled()) {
			log.info(mongoContext.getSql());
		}

		// if (null == this.incrementKey) {
		// int count = mongoContext.executeInsert(this);
		// if (null != this.resultKey) {
		// Ognl.setValue(arg, this.resultKey, count);
		// }
		// } else {
		// InsertReturn insertReturn = mongoContext.executeInsertReturn(this);
		// if (null != this.resultKey) {
		// Ognl.setValue(arg, this.resultKey, insertReturn.getRowCount());
		// }
		// Object columns = insertReturn.getColumns();
		// Ognl.setValue(arg, this.incrementKey, columns);
		// }

		Object result = mongoContext.executeInsert(this);
		if (null != this.resultKey) {
			Ognl.setValue(arg, this.resultKey, result);
		}

		if (log.isInfoEnabled()) {
			log.info("mongo execution time: " + getSlowServiceLog(startTime));
		}

		if (null != cacheClean) {
			cacheClean.removeObject(arg);
		}

		return true;
	}

}
