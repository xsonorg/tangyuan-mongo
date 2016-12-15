package org.xson.tangyuan.executor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.xson.common.object.XCO;
import org.xson.tangyuan.datasource.MongoSupport;
import org.xson.tangyuan.executor.sql.DeleteVo;
import org.xson.tangyuan.executor.sql.InsertVo;
import org.xson.tangyuan.executor.sql.SelectVo;
import org.xson.tangyuan.executor.sql.SqlParser;
import org.xson.tangyuan.executor.sql.UpdateVo;
import org.xson.tangyuan.mapping.MappingVo;
import org.xson.tangyuan.util.BSONUtil;

import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

public class MongoActuator {

	private SqlParser sqlParser = new SqlParser();

	public List<Map<String, Object>> selectAllMap(String dsKey, String sql, MappingVo resultMap, Integer fetchSize) {
		SelectVo selectVo = (SelectVo) sqlParser.parse(sql);
		DBCollection collection = MongoSupport.getCollection(dsKey, selectVo.getTable());
		DBCursor cursor = selectVo.selectSet(collection);
		return getResults(cursor, resultMap);
	}

	public List<XCO> selectAllXCO(String dsKey, String sql, MappingVo resultMap, Integer fetchSize) {
		SelectVo selectVo = (SelectVo) sqlParser.parse(sql);
		DBCollection collection = MongoSupport.getCollection(dsKey, selectVo.getTable());
		DBCursor cursor = selectVo.selectSet(collection);
		return getXCOResults(cursor, resultMap);
	}

	public Map<String, Object> selectOneMap(String dsKey, String sql, MappingVo resultMap, Integer fetchSize) {
		SelectVo selectVo = (SelectVo) sqlParser.parse(sql);
		DBCollection collection = MongoSupport.getCollection(dsKey, selectVo.getTable());
		DBObject result = selectVo.selectOne(collection);
		if (null != result) {
			return getResults(result, resultMap);
		}
		return null;
	}

	public XCO selectOneXCO(String dsKey, String sql, MappingVo resultMap, Integer fetchSize) {
		SelectVo selectVo = (SelectVo) sqlParser.parse(sql);
		DBCollection collection = MongoSupport.getCollection(dsKey, selectVo.getTable());
		DBObject result = selectVo.selectOne(collection);
		if (null != result) {
			return getXCOResults(result, resultMap);
		}
		return null;
	}

	public List<Map<String, Object>> selectAll(String dsKey, String sql) {
		SelectVo selectVo = (SelectVo) sqlParser.parse(sql);
		DBCollection collection = MongoSupport.getCollection(dsKey, selectVo.getTable());
		DBCursor cursor = selectVo.selectSet(collection);
		return getResults(cursor, null);
	}

	public Map<String, Object> selectOne(String dsKey, String sql) {
		SelectVo selectVo = (SelectVo) sqlParser.parse(sql);
		DBCollection collection = MongoSupport.getCollection(dsKey, selectVo.getTable());
		DBObject result = selectVo.selectOne(collection);
		if (null != result) {
			return getResults(result, null);
		}
		return null;
	}

	public Object selectVar(String dsKey, String sql) {
		SelectVo selectVo = (SelectVo) sqlParser.parse(sql);
		DBCollection collection = MongoSupport.getCollection(dsKey, selectVo.getTable());
		return selectVo.selectVar(collection);
	}

	public Object insert(String dsKey, String sql) {
		InsertVo insertVo = (InsertVo) sqlParser.parse(sql);
		DBCollection collection = MongoSupport.getCollection(dsKey, insertVo.getTable());
		return insertVo.insert(collection);
	}

	// public InsertReturn insertReturn(String dsKey, String sql) {
	// InsertVo insertVo = (InsertVo) sqlParser.parse(sql);
	// DBCollection collection = MongoSupport.getCollection(dsKey, insertVo.getTable());
	// int rowCount = insertVo.insert(collection);
	// return new InsertReturn(rowCount, null);
	// // DBCollection collection = db.getCollection("user");
	// // DBObject object = new BasicDBObject();
	// // object.put("name", "aaa");
	// // object.put("age", 11);
	// // collection.save(object);搜索
	// // System.out.println(object.get("_id"));
	// // /ObjectId id = (ObjectId)doc.get( "_id" );
	// }

	public int update(String dsKey, String sql) {
		UpdateVo updateVo = (UpdateVo) sqlParser.parse(sql);
		DBCollection collection = MongoSupport.getCollection(dsKey, updateVo.getTable());
		return updateVo.update(collection);
	}

	public int delete(String dsKey, String sql) {
		DeleteVo deleteVo = (DeleteVo) sqlParser.parse(sql);
		DBCollection collection = MongoSupport.getCollection(dsKey, deleteVo.getTable());
		return deleteVo.delete(collection);
	}

	private Map<String, Object> getResults(DBObject result, MappingVo resultMap) {
		Map<String, Object> row = new HashMap<String, Object>();
		if (null == resultMap) {
			for (String key : result.keySet()) {
				// row.put(key, result.get(key));
				BSONUtil.setObjectValue(row, key, result.get(key));
			}
		} else {
			for (String key : result.keySet()) {
				// row.put(resultMap.getProperty(key), result.get(key));
				BSONUtil.setObjectValue(row, resultMap.getProperty(key), result.get(key));
			}
		}
		return row;
	}

	private XCO getXCOResults(DBObject result, MappingVo resultMap) {
		XCO row = new XCO();
		if (null == resultMap) {
			for (String key : result.keySet()) {
				// row.setObjectValue(key, result.get(key));
				BSONUtil.setObjectValue(row, key, result.get(key));
			}
		} else {
			for (String key : result.keySet()) {
				// row.setObjectValue(resultMap.getProperty(key), result.get(key));
				BSONUtil.setObjectValue(row, resultMap.getProperty(key), result.get(key));
			}
		}
		return row;
	}

	private List<Map<String, Object>> getResults(DBCursor cursor, MappingVo resultMap) {
		List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
		try {
			if (null == resultMap) {
				while (cursor.hasNext()) {
					DBObject bson = cursor.next();
					Map<String, Object> row = new HashMap<String, Object>();
					for (String key : bson.keySet()) {
						// row.put(key, bson.get(key));
						BSONUtil.setObjectValue(row, key, bson.get(key));
					}
					list.add(row);
				}
			} else {
				while (cursor.hasNext()) {
					DBObject bson = cursor.next();
					Map<String, Object> row = new HashMap<String, Object>();
					for (String key : bson.keySet()) {
						// row.put(resultMap.getProperty(key), bson.get(key));
						BSONUtil.setObjectValue(row, resultMap.getProperty(key), bson.get(key));
					}
					list.add(row);
				}
			}
			return list;
		} finally {
			cursor.close();
		}
	}

	private List<XCO> getXCOResults(DBCursor cursor, MappingVo resultMap) {
		List<XCO> list = new ArrayList<XCO>();
		try {
			if (null == resultMap) {
				while (cursor.hasNext()) {
					DBObject bson = cursor.next();
					XCO row = new XCO();
					for (String key : bson.keySet()) {
						Object obj = bson.get(key);
						// System.out.println(bson.get(key).getClass() + ":" + bson.get(key));
						// if (obj instanceof org.bson.types.ObjectId) {
						// row.setObjectValue(key, obj.toString());
						// } else {
						// row.setObjectValue(key, obj);
						// }
						BSONUtil.setObjectValue(row, key, obj);
					}
					list.add(row);
				}
			} else {
				while (cursor.hasNext()) {
					DBObject bson = cursor.next();
					XCO row = new XCO();
					for (String key : bson.keySet()) {
						// row.setObjectValue(resultMap.getProperty(key), bson.get(key));
						BSONUtil.setObjectValue(row, resultMap.getProperty(key), bson.get(key));
					}
					list.add(row);
				}
			}
			return list;
		} finally {
			cursor.close();
		}
	}

}
