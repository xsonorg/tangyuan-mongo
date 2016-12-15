package org.xson.tangyuan.executor.sql.condition;

import org.xson.tangyuan.executor.sql.SqlParseException;
import org.xson.tangyuan.executor.sql.SqlParser;
import org.xson.tangyuan.executor.sql.ValueVo;
import org.xson.tangyuan.executor.sql.ValueVo.ValueType;
import org.xson.tangyuan.executor.sql.WhereCondition;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

/**
 * a>b
 */
public class MoreThanCondition extends WhereCondition {

	private ValueVo value;

	@Override
	public void setValue(Object value) {
		this.value = (ValueVo) value;
	}

	@Override
	public void check() {
		if (null == value) {
			throw new SqlParseException("Illegal more than condition");
		}
	}

	@Override
	public void toSQL(StringBuilder builder) {
		builder.append(this.name);
		builder.append(SqlParser.BLANK_MARK);
		builder.append(">");
		builder.append(SqlParser.BLANK_MARK);
		if (ValueType.STRING == value.getType()) {
			builder.append('\'');
			builder.append(value.getSqlValue());
			builder.append('\'');
		} else {
			builder.append(value.getSqlValue());
		}
	}

	@Override
	public void setQuery(DBObject query, BasicDBList orList) {
		if (null == orList) {
			query.put(this.name, new BasicDBObject("$gt", value.getValue()));
		} else {
			orList.add(new BasicDBObject(this.name, new BasicDBObject("$gt", value.getValue())));
		}
	}
}
