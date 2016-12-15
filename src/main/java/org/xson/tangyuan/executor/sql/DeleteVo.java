package org.xson.tangyuan.executor.sql;

import org.xson.tangyuan.TangYuanMongoContainer;
import org.xson.tangyuan.logging.Log;
import org.xson.tangyuan.logging.LogFactory;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.WriteResult;

//DELETE FROM 表名称 WHERE 列名称 = 值

public class DeleteVo implements SqlVo {

	private static Log		log	= LogFactory.getLog(DeleteVo.class);

	private String			table;

	private WhereCondition	condition;

	public void setTable(String table) {
		this.table = table;
	}

	public void setCondition(WhereCondition condition) {
		this.condition = condition;
	}

	@Override
	public String getTable() {
		return this.table;
	}

	public void check() {
		if (null == table || 0 == table.length()) {
			throw new SqlParseException("delete [table] is null");
		}
		if (null != condition) {
			condition.check();
		}
	}

	@Override
	public String toSQL() {
		StringBuilder builder = new StringBuilder();
		builder.append(SqlParser.DELETE_MARK);
		builder.append(SqlParser.BLANK_MARK);
		builder.append(this.table);
		if (null != condition) {
			builder.append(SqlParser.BLANK_MARK);
			builder.append(SqlParser.WHERE_MARK);
			builder.append(SqlParser.BLANK_MARK);
			condition.toSQL(builder);
		}
		return builder.toString();
	}

	public int delete(DBCollection collection) {
		DBObject query = new BasicDBObject();
		if (null != condition) {
			this.condition.setQuery(query, null);
		}

		log(query);

		// WriteResult result = collection.remove(query, WriteConcern.ACKNOWLEDGED);
		WriteResult result = collection.remove(query, TangYuanMongoContainer.getInstance().getDefaultWriteConcern());
		// System.out.println(query.toString());
		return result.getN();
	}

	private void log(DBObject query) {
		if (log.isInfoEnabled()) {
			if (null != query) {
				log.info("query:" + query.toString());
			}
		}
	}
}
