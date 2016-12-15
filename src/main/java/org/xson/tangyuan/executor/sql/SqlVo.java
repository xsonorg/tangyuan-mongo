package org.xson.tangyuan.executor.sql;

public interface SqlVo {

	public String toSQL();

	public String getTable();

	public void check();
}
