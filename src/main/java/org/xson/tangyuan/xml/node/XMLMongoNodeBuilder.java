package org.xson.tangyuan.xml.node;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xson.common.object.XCO;
import org.xson.tangyuan.TangYuanContainer;
import org.xson.tangyuan.TangYuanMongoContainer;
import org.xson.tangyuan.cache.vo.CacheCleanVo;
import org.xson.tangyuan.cache.vo.CacheUseVo;
import org.xson.tangyuan.cache.vo.CacheVo;
import org.xson.tangyuan.logging.Log;
import org.xson.tangyuan.logging.LogFactory;
import org.xson.tangyuan.mapping.MappingVo;
import org.xson.tangyuan.ognl.vars.Variable;
import org.xson.tangyuan.ognl.vars.parser.LogicalExprParser;
import org.xson.tangyuan.ognl.vars.parser.NormalParser;
import org.xson.tangyuan.util.ClassUtils;
import org.xson.tangyuan.util.DateUtils;
import org.xson.tangyuan.util.NumberUtils;
import org.xson.tangyuan.util.StringUtils;
import org.xson.tangyuan.util.TYUtils;
import org.xson.tangyuan.xml.XPathParser;
import org.xson.tangyuan.xml.XmlConfigurationBuilder;
import org.xson.tangyuan.xml.XmlMongoConfigBuilder;
import org.xson.tangyuan.xml.XmlMongoMapperBuilder;
import org.xson.tangyuan.xml.XmlNodeWrapper;
import org.xson.tangyuan.xml.XmlParseException;
import org.xson.tangyuan.xml.node.CallNode.CallMode;
import org.xson.tangyuan.xml.node.CallNode.CallNodeParameterItem;
import org.xson.tangyuan.xml.node.ReturnNode.ReturnItem;

public class XMLMongoNodeBuilder {

	private Log							log						= LogFactory.getLog(getClass());
	private XPathParser					parser					= null;
	private String						ns						= "";

	private XmlConfigurationBuilder		xmlConfigurationBuilder	= null;
	private XmlMongoConfigBuilder		xmlMongoConfigBuilder	= null;
	private XmlMongoMapperBuilder		xmlMapperBuilder		= null;
	private XmlNodeWrapper				root					= null;
	/**
	 * <SQL>节点MAP
	 */
	private Map<String, TangYuanNode>	integralRefMap			= null;
	/**
	 * 服务节点
	 */
	private Map<String, Integer>		integralServiceMap		= null;
	private String						dsKeyWithSqlService		= null;
	private Class<?>					serviceResultType		= null;

	public XMLMongoNodeBuilder(InputStream inputStream, XmlMongoConfigBuilder xmlMongoConfigBuilder, XmlConfigurationBuilder xmlConfigurationBuilder,
			XmlMongoMapperBuilder xmlMapperBuilder, Map<String, TangYuanNode> integralRefMap, Map<String, Integer> integralServiceMap) {
		this.parser = new XPathParser(inputStream);
		this.xmlConfigurationBuilder = xmlConfigurationBuilder;
		this.xmlMongoConfigBuilder = xmlMongoConfigBuilder;
		this.xmlMapperBuilder = xmlMapperBuilder;
		this.integralRefMap = integralRefMap;
		this.integralServiceMap = integralServiceMap;
	}

	private boolean isEmpty(String data) {
		if (null == data || 0 == data.trim().length()) {
			return true;
		}
		return false;
	}

	// private String getFullId(String id) {
	// if (null == ns || "".equals(ns)) {
	// return id;
	// }
	// return ns + "." + id;
	// }

	private String getFullId(String id) {
		return TYUtils.getFullId(ns, id);
	}

	// private void registerService(List<AbstractMongoNode> list, String nodeName) {
	// // TODO 这里还要排重
	// for (AbstractMongoNode node : list) {
	// TangYuanContainer.getInstance().addService(node);
	// log.info("add <" + nodeName + "> node: " + node.getServiceKey());
	// }
	// }

	protected void registerService(List<AbstractMongoNode> list, String nodeName) {
		boolean result = TangYuanContainer.getInstance().hasLicenses();
		for (AbstractMongoNode node : list) {
			if (null != TangYuanContainer.getInstance().getService(node.getServiceKey())) {
				throw new XmlParseException("重复的服务:" + node.getServiceKey());
			}

			if (result) {
				TangYuanContainer.getInstance().addService(node);
			} else {
				log.info("&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&");
				if (NumberUtils.randomSuccess()) {
					TangYuanContainer.getInstance().addService(node);
				}
			}
			log.info("add <" + nodeName + "> node: " + node.getServiceKey());
		}
	}

	private String getResultKey(String str) {
		if (null != str && str.length() > 2 && str.startsWith("{") && str.endsWith("}")) {
			return str.substring(1, str.length() - 1);
		}
		return null;
	}

	private boolean checkVar(String str) {
		if (null != str && str.length() > 2 && str.startsWith("{") && str.endsWith("}")) {
			return true;
		}
		return false;
	}

	private String getRealVal(String str) {
		return str.substring(1, str.length() - 1);
	}

	private void existingService(String id) {
		String fullId = getFullId(id);
		if (null != integralServiceMap.get(fullId)) {
			throw new XmlParseException("重复的节点:" + fullId);
		}
		if (null != integralRefMap.get(fullId)) {
			throw new XmlParseException("重复的节点:" + fullId);
		}
		integralServiceMap.put(fullId, 1);
	}

	/**
	 * 解析: ID:xxx; key:xxx; time:1000; ignore:a,b
	 */
	private CacheUseVo parseCacheUse(String cacheUse, String service) {
		CacheUseVo cacheUseVo = null;
		String[] array = cacheUse.split(";");
		if (array.length > 0) {
			Map<String, String> map = new HashMap<String, String>();
			for (int i = 0; i < array.length; i++) {
				String[] item = array[i].split(":");
				map.put(item[0].trim().toUpperCase(), item[1].trim());
			}
			CacheVo cacheVo = null;
			if (map.containsKey("id".toUpperCase())) {
				cacheVo = xmlConfigurationBuilder.getCacheVoMap().get(map.get("id".toUpperCase()));
			} else {
				cacheVo = xmlConfigurationBuilder.getDefaultCacheVo();
			}
			if (null == cacheVo) {
				throw new XmlParseException("不存在的cache: " + cacheUse);
			}

			String key = map.get("key".toUpperCase());
			if (null == key) {
				throw new XmlParseException("不存在的cache.key: " + cacheUse);
			}
			// TODO: 可用expire代替
			Integer time = null;
			if (map.containsKey("time".toUpperCase())) {
				time = Integer.parseInt(map.get("time".toUpperCase()));
			}
			String[] ignore = null;
			if (map.containsKey("ignore".toUpperCase())) {
				ignore = map.get("ignore".toUpperCase()).split(",");
			}
			cacheUseVo = new CacheUseVo(cacheVo, key, time, ignore, service);
		}
		return cacheUseVo;
	}

	/**
	 * 解析: ID:xxx; key:xxx; ignore=a,b
	 */
	private CacheCleanVo parseCacheClean(String cacheUse, String service) {
		CacheCleanVo cacheCleanVo = null;
		String[] array = cacheUse.split(";");
		if (array.length > 0) {
			Map<String, String> map = new HashMap<String, String>();
			for (int i = 0; i < array.length; i++) {
				String[] item = array[i].split(":");
				map.put(item[0].trim().toUpperCase(), item[1].trim());
			}
			CacheVo cacheVo = null;
			if (map.containsKey("id".toUpperCase())) {
				cacheVo = xmlConfigurationBuilder.getCacheVoMap().get(map.get("id".toUpperCase()));
			} else {
				cacheVo = xmlConfigurationBuilder.getDefaultCacheVo();
			}
			if (null == cacheVo) {
				throw new XmlParseException("不存在的cache:" + cacheUse);
			}

			String key = map.get("key".toUpperCase());
			if (null == key) {
				throw new XmlParseException("不存在的cache.key:" + cacheUse);
			}

			String[] ignore = null;
			if (map.containsKey("ignore".toUpperCase())) {
				ignore = map.get("ignore".toUpperCase()).split(",");
			}
			cacheCleanVo = new CacheCleanVo(cacheVo, key, ignore, service);
		}
		return cacheCleanVo;
	}

	public void parseRef() {
		this.root = parser.evalNode("/mongoservices");
		this.ns = this.root.getStringAttribute("ns", "");
		buildRefNode(this.root.evalNodes("sql"));
	}

	public void parseService() {
		configurationElement(this.root);
	}

	private void buildRefNode(List<XmlNodeWrapper> contexts) {
		for (XmlNodeWrapper context : contexts) {
			String id = StringUtils.trim(context.getStringAttribute("id")); // xml
																			// validation
			String fullId = getFullId(id);
			if (null == integralRefMap.get(fullId)) {
				TangYuanNode sqlNode = parseNode(context, false);
				if (null != sqlNode) {
					integralRefMap.put(fullId, sqlNode);
					log.info("add <sql> node: " + fullId);
				}
			} else {
				throw new XmlParseException("重复的<sql>节点:" + id);
			}
		}
	}

	private void configurationElement(XmlNodeWrapper context) {
		try {

			List<AbstractMongoNode> selectSetList = buildSelectSetNodes(context.evalNodes("selectSet"));
			List<AbstractMongoNode> selectOneList = buildSelectOneNodes(context.evalNodes("selectOne"));
			List<AbstractMongoNode> selectVarList = buildSelectVarNodes(context.evalNodes("selectVar"));
			List<AbstractMongoNode> insertList = buildInsertNodes(context.evalNodes("insert"));
			List<AbstractMongoNode> updateList = buildUpdateNodes(context.evalNodes("update"));
			List<AbstractMongoNode> deleteList = buildDeleteNodes(context.evalNodes("delete"));
			List<AbstractMongoNode> sqlServiceList = buildSqlServiceNodes(context.evalNodes("mongo-service"));

			registerService(selectSetList, "selectSet");
			registerService(selectOneList, "selectOne");
			registerService(selectVarList, "selectVar");
			registerService(insertList, "insert");
			registerService(updateList, "update");
			registerService(deleteList, "delete");
			registerService(sqlServiceList, "mongo-service");

		} catch (Exception e) {
			throw new XmlParseException(e);
		}
	}

	private TangYuanNode parseNode(XmlNodeWrapper context, boolean internal) {
		List<TangYuanNode> contents = parseDynamicTags(context);
		int size = contents.size();
		TangYuanNode sqlNode = null;
		if (size == 1) {
			sqlNode = contents.get(0);
		} else if (size > 1) {
			sqlNode = new MixedNode(contents);
		} else {
			log.warn("节点内容为空, 将被忽略:" + context.getName());
		}
		return sqlNode;
	}

	private List<TangYuanNode> parseDynamicTags(XmlNodeWrapper node) {
		List<TangYuanNode> contents = new ArrayList<TangYuanNode>();
		NodeList children = node.getNode().getChildNodes();
		for (int i = 0; i < children.getLength(); i++) {
			XmlNodeWrapper child = node.newXMlNode(children.item(i));
			if (child.getNode().getNodeType() == Node.CDATA_SECTION_NODE || child.getNode().getNodeType() == Node.TEXT_NODE) {
				String data = child.getStringBody("");
				if (isEmpty(data)) {
					continue;
				}
				// 使用新的sqlText节点
				contents.add(new MongoTextNode(data));
				// log.info("-----------data:" + data);
			} else if (child.getNode().getNodeType() == Node.ELEMENT_NODE) {
				String nodeName = child.getNode().getNodeName();
				// log.info("-----------name:" + nodeName);
				NodeHandler handler = nodeHandlers.get(nodeName);
				if (handler == null) {
					throw new XmlParseException("Unknown element <" + nodeName + "> in SQL statement.");
				}
				handler.handleNode(child, contents);
			}
		}
		return contents;
	}

	class SelectResult {
		Class<?>	resultType;
		MappingVo	resultMap;

		SelectResult(Class<?> resultType, MappingVo resultMap) {
			this.resultType = resultType;
			this.resultMap = resultMap;
		}
	}

	private SelectResult parseSelectResult(String _resultType, String _resultMap) {

		Class<?> resultType = null;
		MappingVo resultMap = null;
		if (null == _resultType && null == _resultMap) {
			// 都没有值的情况下
			resultType = TangYuanContainer.getInstance().getDefaultResultType();// 这里是简单服务,直接只用系统默认即可
		} else if (null != _resultType && null != _resultMap) {
			// 都存在值的情况下

			// resultType处理
			if ("map".equalsIgnoreCase(_resultType)) {
				resultType = Map.class;
			} else if ("xco".equalsIgnoreCase(_resultType)) {
				resultType = XCO.class;
			} else {
				resultType = ClassUtils.forName(_resultType);
			}

			// resultMap处理
			resultMap = xmlMapperBuilder.getMappingVoMap().get(_resultMap);
			if (null == resultMap) {
				throw new XmlParseException("不存在的ResultMap:" + _resultMap);
			}

			// 检测是否冲突
			if (null != resultMap.getBeanClass() && resultType != resultMap.getBeanClass()) {
				throw new XmlParseException("ResultMap[" + resultMap.getBeanClass() + "] and ResultType[" + resultType + "]类型冲突");
			}

		} else if (null == _resultType && null != _resultMap) {
			resultMap = xmlMapperBuilder.getMappingVoMap().get(_resultMap);
			if (null == resultMap) {
				throw new XmlParseException("不存在的ResultMap:" + _resultMap);
			}
			if (null == resultMap.getBeanClass()) {
				resultType = TangYuanContainer.getInstance().getDefaultResultType();
			}
			// 具体的类型看resultMap.type
		} else if (null != _resultType && null == _resultMap) {
			if ("map".equalsIgnoreCase(_resultType)) {
				resultType = Map.class;
			} else if ("xco".equalsIgnoreCase(_resultType)) {
				resultType = XCO.class;
			} else {
				resultType = ClassUtils.forName(_resultType);
				// 默认Bean Result Mapping
			}
		}
		return new SelectResult(resultType, resultMap);
	}

	private List<AbstractMongoNode> buildSelectSetNodes(List<XmlNodeWrapper> contexts) {
		List<AbstractMongoNode> list = new ArrayList<AbstractMongoNode>();
		for (XmlNodeWrapper context : contexts) {
			TangYuanNode sqlNode = parseNode(context, false);
			if (null != sqlNode) {
				String id = StringUtils.trim(context.getStringAttribute("id")); // xml
																				// validation
				existingService(id);

				String _resultType = StringUtils.trim(context.getStringAttribute("resultType"));
				String _resultMap = StringUtils.trim(context.getStringAttribute("resultMap"));
				SelectResult selectResult = parseSelectResult(_resultType, _resultMap);

				String _fetchSize = StringUtils.trim(context.getStringAttribute("fetchSize")); // xml
																								// validation
				Integer fetchSize = null;
				if (null != _fetchSize) {
					fetchSize = Integer.valueOf(_fetchSize);
				}
				String dsKey = StringUtils.trim(context.getStringAttribute("dsKey"));
				if (null == dsKey) {
					dsKey = xmlMongoConfigBuilder.getDefaultDsKey();
					if (null == dsKey) {
						throw new XmlParseException("无效的dsKey");
					}
				} else {
					if (!TangYuanMongoContainer.getInstance().getDataSourceManager().isValidDsKey(dsKey)) {
						throw new XmlParseException("无效的dsKey:" + dsKey);
					}
				}

				// String txRef = StringUtils.trim(context.getStringAttribute("txRef"));
				// XTransactionDefinition txDef = this.xmlConfigurationBuilder.getTransactionMatcher().getTransactionDefinition(txRef, id,
				// "selectSet");
				// if (null == txDef) {
				// throw new XmlParseException("不存在的事务:" + id);
				// }

				// ID:xxx; key:xxx; time=1000; ignore=a,b
				String _cacheUse = StringUtils.trim(context.getStringAttribute("cacheUse"));
				CacheUseVo cacheUse = null;
				if (null != _cacheUse && _cacheUse.length() > 0) {
					cacheUse = parseCacheUse(_cacheUse, getFullId(id));
				}

				// SelectSetNode selectSetNode = new SelectSetNode(id, ns, getFullId(id), selectResult.resultType, selectResult.resultMap, dsKey,
				// fetchSize, txDef, sqlNode, cacheUse);

				MongoSelectSetNode selectSetNode = new MongoSelectSetNode(id, ns, getFullId(id), selectResult.resultType, selectResult.resultMap,
						dsKey, fetchSize, sqlNode, cacheUse);

				list.add(selectSetNode);
			}
		}
		return list;
	}

	private List<AbstractMongoNode> buildSelectOneNodes(List<XmlNodeWrapper> contexts) {
		List<AbstractMongoNode> list = new ArrayList<AbstractMongoNode>();
		for (XmlNodeWrapper context : contexts) {
			TangYuanNode sqlNode = parseNode(context, false);
			if (null != sqlNode) {
				String id = StringUtils.trim(context.getStringAttribute("id")); // xml
																				// validation
				existingService(id);

				String _resultType = StringUtils.trim(context.getStringAttribute("resultType"));
				String _resultMap = StringUtils.trim(context.getStringAttribute("resultMap"));
				SelectResult selectResult = parseSelectResult(_resultType, _resultMap);

				String dsKey = StringUtils.trim(context.getStringAttribute("dsKey"));
				if (null == dsKey) {
					dsKey = xmlMongoConfigBuilder.getDefaultDsKey();
					if (null == dsKey) {
						throw new XmlParseException("无效的dsKey");
					}
				} else {
					if (!TangYuanMongoContainer.getInstance().getDataSourceManager().isValidDsKey(dsKey)) {
						throw new XmlParseException("无效的dsKey:" + dsKey);
					}
				}

				// String txRef = StringUtils.trim(context.getStringAttribute("txRef"));
				// XTransactionDefinition txDef = this.xmlConfigurationBuilder.getTransactionMatcher().getTransactionDefinition(txRef, id,
				// "selectOne");
				// if (null == txDef) {
				// throw new XmlParseException("不存在的事务:" + id);
				// }

				String _cacheUse = StringUtils.trim(context.getStringAttribute("cacheUse"));
				CacheUseVo cacheUse = null;
				if (null != _cacheUse && _cacheUse.length() > 0) {
					cacheUse = parseCacheUse(_cacheUse, getFullId(id));
				}

				// SelectOneNode selectOneNode = new SelectOneNode(id, ns, getFullId(id), selectResult.resultType, selectResult.resultMap, dsKey,
				// txDef,
				// sqlNode, cacheUse);

				MongoSelectOneNode selectOneNode = new MongoSelectOneNode(id, ns, getFullId(id), selectResult.resultType, selectResult.resultMap,
						dsKey, sqlNode, cacheUse);

				list.add(selectOneNode);
			}
		}
		return list;
	}

	private List<AbstractMongoNode> buildSelectVarNodes(List<XmlNodeWrapper> contexts) {
		List<AbstractMongoNode> list = new ArrayList<AbstractMongoNode>();
		for (XmlNodeWrapper context : contexts) {
			TangYuanNode sqlNode = parseNode(context, false);
			if (null != sqlNode) {
				String id = StringUtils.trim(context.getStringAttribute("id")); // xml
																				// validation
				existingService(id);

				String dsKey = StringUtils.trim(context.getStringAttribute("dsKey"));
				if (null == dsKey) {
					dsKey = xmlMongoConfigBuilder.getDefaultDsKey();
					if (null == dsKey) {
						throw new XmlParseException("无效的dsKey");
					}
				} else {
					if (!TangYuanMongoContainer.getInstance().getDataSourceManager().isValidDsKey(dsKey)) {
						throw new XmlParseException("无效的dsKey:" + dsKey);
					}
				}

				// String txRef = StringUtils.trim(context.getStringAttribute("txRef"));
				// XTransactionDefinition txDef = this.xmlConfigurationBuilder.getTransactionMatcher().getTransactionDefinition(txRef, id,
				// "selectVar");
				// if (null == txDef) {
				// throw new XmlParseException("不存在的事务:" + id);
				// }

				String _cacheUse = StringUtils.trim(context.getStringAttribute("cacheUse"));
				CacheUseVo cacheUse = null;
				if (null != _cacheUse && _cacheUse.length() > 0) {
					cacheUse = parseCacheUse(_cacheUse, getFullId(id));
				}

				MongoSelectVarNode selectVarNode = new MongoSelectVarNode(id, ns, getFullId(id), dsKey, sqlNode, cacheUse);
				list.add(selectVarNode);
			}
		}
		return list;
	}

	private List<AbstractMongoNode> buildInsertNodes(List<XmlNodeWrapper> contexts) {
		List<AbstractMongoNode> list = new ArrayList<AbstractMongoNode>();
		for (XmlNodeWrapper context : contexts) {
			TangYuanNode sqlNode = parseNode(context, false);
			if (null != sqlNode) {
				String id = StringUtils.trim(context.getStringAttribute("id")); // xml
																				// validation
				existingService(id);

				// String _resultType = StringUtils.trim(context.getStringAttribute("resultType"));// 仅作标示作用
				// Class<?> resultType = null;
				// if (null != _resultType) {
				// resultType = Object.class;
				// }

				String dsKey = StringUtils.trim(context.getStringAttribute("dsKey"));
				if (null == dsKey) {
					dsKey = xmlMongoConfigBuilder.getDefaultDsKey();
					if (null == dsKey) {
						throw new XmlParseException("无效的dsKey");
					}
				} else {
					if (!TangYuanMongoContainer.getInstance().getDataSourceManager().isValidDsKey(dsKey)) {
						throw new XmlParseException("无效的dsKey:" + dsKey);
					}
				}

				// String txRef = StringUtils.trim(context.getStringAttribute("txRef"));
				// XTransactionDefinition txDef = this.xmlConfigurationBuilder.getTransactionMatcher().getTransactionDefinition(txRef, id, "insert");
				// if (null == txDef) {
				// throw new XmlParseException("不存在的事务:" + id);
				// }

				String _cacheClean = StringUtils.trim(context.getStringAttribute("cacheClean"));
				CacheCleanVo cacheClean = null;
				if (null != _cacheClean && _cacheClean.length() > 0) {
					cacheClean = parseCacheClean(_cacheClean, getFullId(id));
				}

				// MongoInsertNode insertNode = new MongoInsertNode(id, ns, getFullId(id), resultType, dsKey, sqlNode, cacheClean);

				MongoInsertNode insertNode = new MongoInsertNode(id, ns, getFullId(id), dsKey, sqlNode, cacheClean);

				list.add(insertNode);
			}
		}
		return list;
	}

	private List<AbstractMongoNode> buildUpdateNodes(List<XmlNodeWrapper> contexts) {
		List<AbstractMongoNode> list = new ArrayList<AbstractMongoNode>();
		for (XmlNodeWrapper context : contexts) {
			TangYuanNode sqlNode = parseNode(context, false);
			if (null != sqlNode) {
				String id = StringUtils.trim(context.getStringAttribute("id")); // xml
																				// validation
				existingService(id);

				String dsKey = StringUtils.trim(context.getStringAttribute("dsKey"));
				if (null == dsKey) {
					dsKey = xmlMongoConfigBuilder.getDefaultDsKey();
					if (null == dsKey) {
						throw new XmlParseException("无效的dsKey");
					}
				} else {
					if (!TangYuanMongoContainer.getInstance().getDataSourceManager().isValidDsKey(dsKey)) {
						throw new XmlParseException("无效的dsKey:" + dsKey);
					}
				}

				// String txRef = StringUtils.trim(context.getStringAttribute("txRef"));
				// XTransactionDefinition txDef = this.xmlConfigurationBuilder.getTransactionMatcher().getTransactionDefinition(txRef, id, "update");
				// if (null == txDef) {
				// throw new XmlParseException("不存在的事务:" + id);
				// }

				String _cacheClean = StringUtils.trim(context.getStringAttribute("cacheClean"));
				CacheCleanVo cacheClean = null;
				if (null != _cacheClean && _cacheClean.length() > 0) {
					cacheClean = parseCacheClean(_cacheClean, getFullId(id));
				}

				MongoUpdateNode updateNode = new MongoUpdateNode(id, ns, getFullId(id), dsKey, sqlNode, cacheClean);
				list.add(updateNode);
			}
		}
		return list;
	}

	private List<AbstractMongoNode> buildDeleteNodes(List<XmlNodeWrapper> contexts) {
		List<AbstractMongoNode> list = new ArrayList<AbstractMongoNode>();
		for (XmlNodeWrapper context : contexts) {
			TangYuanNode sqlNode = parseNode(context, false);
			if (null != sqlNode) {
				String id = StringUtils.trim(context.getStringAttribute("id")); // xml
																				// validation
				existingService(id);

				String dsKey = StringUtils.trim(context.getStringAttribute("dsKey"));
				if (null == dsKey) {
					dsKey = xmlMongoConfigBuilder.getDefaultDsKey();
					if (null == dsKey) {
						throw new XmlParseException("无效的dsKey");
					}
				} else {
					if (!TangYuanMongoContainer.getInstance().getDataSourceManager().isValidDsKey(dsKey)) {
						throw new XmlParseException("无效的dsKey:" + dsKey);
					}
				}

				// String txRef = StringUtils.trim(context.getStringAttribute("txRef"));
				// XTransactionDefinition txDef = this.xmlConfigurationBuilder.getTransactionMatcher().getTransactionDefinition(txRef, id, "delete");
				// if (null == txDef) {
				// throw new XmlParseException("不存在的事务:" + id);
				// }

				String _cacheClean = StringUtils.trim(context.getStringAttribute("cacheClean"));
				CacheCleanVo cacheClean = null;
				if (null != _cacheClean && _cacheClean.length() > 0) {
					cacheClean = parseCacheClean(_cacheClean, getFullId(id));
				}

				MongoDeleteNode deleteNode = new MongoDeleteNode(id, ns, getFullId(id), dsKey, sqlNode, cacheClean);
				list.add(deleteNode);
			}
		}
		return list;
	}

	private List<AbstractMongoNode> buildSqlServiceNodes(List<XmlNodeWrapper> contexts) {
		List<AbstractMongoNode> list = new ArrayList<AbstractMongoNode>();
		for (XmlNodeWrapper context : contexts) {
			String id = StringUtils.trim(context.getStringAttribute("id")); // xml
																			// validation
			existingService(id);

			// String txRef = StringUtils.trim(context.getStringAttribute("txRef"));
			// XTransactionDefinition txDef = this.xmlConfigurationBuilder.getTransactionMatcher().getTransactionDefinition(txRef, id, "sql-service");
			// if (null == txDef) {
			// throw new XmlParseException("不存在的事务:" + id);
			// }

			String dsKey = StringUtils.trim(context.getStringAttribute("dsKey"));
			if (null != dsKey && !TangYuanMongoContainer.getInstance().getDataSourceManager().isValidDsKey(dsKey)) {
				throw new XmlParseException("无效的dsKey:" + dsKey);
			}

			// this.txRefWithSqlService = txRef;
			this.dsKeyWithSqlService = dsKey;

			String _resultType = StringUtils.trim(context.getStringAttribute("resultType"));
			this.serviceResultType = null;
			if ("map".equalsIgnoreCase(_resultType)) {
				this.serviceResultType = Map.class;
			} else if ("xco".equalsIgnoreCase(_resultType)) {
				this.serviceResultType = XCO.class;
			} else {
				this.serviceResultType = TangYuanContainer.getInstance().getDefaultResultType();
			}

			String _cacheUse = StringUtils.trim(context.getStringAttribute("cacheUse"));
			CacheUseVo cacheUse = null;
			if (null != _cacheUse && _cacheUse.length() > 0) {
				cacheUse = parseCacheUse(_cacheUse, getFullId(id));
			}
			String _cacheClean = StringUtils.trim(context.getStringAttribute("cacheClean"));
			CacheCleanVo cacheClean = null;
			if (null != _cacheClean && _cacheClean.length() > 0) {
				cacheClean = parseCacheClean(_cacheClean, getFullId(id));
			}

			TangYuanNode sqlNode = parseNode(context, true);
			if (null != sqlNode) {
				MongoServiceNode serviceNode = new MongoServiceNode(id, this.ns, getFullId(id), dsKey, sqlNode, cacheUse, cacheClean,
						this.serviceResultType);
				list.add(serviceNode);
			}
		}
		return list;
	}

	// ////////////////////////////////////////////////////////////////////////////////////////////////////////

	private interface NodeHandler {
		void handleNode(XmlNodeWrapper nodeToHandle, List<TangYuanNode> targetContents);
	}

	private class IfHandler implements NodeHandler {
		public void handleNode(XmlNodeWrapper nodeToHandle, List<TangYuanNode> targetContents) {

			String test = nodeToHandle.getStringAttribute("test");
			if (null == test) {
				throw new XmlParseException("If Handler test = null");
			}

			List<TangYuanNode> contents = parseDynamicTags(nodeToHandle);
			int size = contents.size();

			IfNode ifNode = null;
			if (1 == size) {
				ifNode = new IfNode(contents.get(0), new LogicalExprParser().parse(test));
			} else if (size > 1) {
				ifNode = new IfNode(new MixedNode(contents), new LogicalExprParser().parse(test));
			} else { // size == 0
				throw new XmlParseException("If Handler contents = null");
			}
			targetContents.add(ifNode);
		}
	}

	private class ElseIfHandler implements NodeHandler {
		public void handleNode(XmlNodeWrapper nodeToHandle, List<TangYuanNode> targetContents) {
			if (0 == targetContents.size()) {
				throw new XmlParseException("ElseIf节点不合法0");
			}
			TangYuanNode previousNode = targetContents.get(targetContents.size() - 1);
			if (!(previousNode instanceof IfNode)) {
				throw new XmlParseException("ElseIf节点之前的节点必须是IF节点");
			}
			String test = nodeToHandle.getStringAttribute("test");
			if (null == test) {
				throw new XmlParseException("If Handler test = null");
			}

			List<TangYuanNode> contents = parseDynamicTags(nodeToHandle);
			int size = contents.size();

			IfNode ifNode = null;
			if (1 == size) {
				ifNode = new IfNode(contents.get(0), new LogicalExprParser().parse(test));
			} else if (size > 1) {
				ifNode = new IfNode(new MixedNode(contents), new LogicalExprParser().parse(test));
			} else {
				throw new XmlParseException("If Handler contents = null");
			}
			((IfNode) previousNode).addElseIfNode(ifNode);
		}
	}

	private class ElseHandler implements NodeHandler {
		public void handleNode(XmlNodeWrapper nodeToHandle, List<TangYuanNode> targetContents) {
			if (0 == targetContents.size()) {
				throw new XmlParseException("ElseIf节点不合法0");
			}
			TangYuanNode previousNode = targetContents.get(targetContents.size() - 1);
			if (!(previousNode instanceof IfNode)) {
				throw new XmlParseException("ElseIf节点不合法1");
			}
			List<TangYuanNode> contents = parseDynamicTags(nodeToHandle);
			int size = contents.size();
			IfNode ifNode = null;
			if (1 == size) {
				ifNode = new IfNode(contents.get(0), null);
			} else if (size > 1) {
				ifNode = new IfNode(new MixedNode(contents), null);
			} else {
				throw new XmlParseException("If Handler contents = null");
			}
			((IfNode) previousNode).addElseNode(ifNode);
		}
	}

	private class IncludeHandler implements NodeHandler {
		public void handleNode(XmlNodeWrapper nodeToHandle, List<TangYuanNode> targetContents) {
			String refKey = nodeToHandle.getStringAttribute("ref"); // xml
																	// validation
			TangYuanNode refNode = integralRefMap.get(refKey);
			if (null == refNode) {
				throw new XmlParseException("refNode is null:" + refKey);
			}
			targetContents.add(refNode);
		}
	}

	private class ForEachHandler implements NodeHandler {
		public void handleNode(XmlNodeWrapper nodeToHandle, List<TangYuanNode> targetContents) {

			String collection = StringUtils.trim(nodeToHandle.getStringAttribute("collection"));
			if (!checkVar(collection)) {
				throw new XmlParseException("ForEach collection 不合法, 应是{xxx}");
			}
			collection = getRealVal(collection);

			String index = StringUtils.trim(nodeToHandle.getStringAttribute("index"));
			if (null != index) {
				if (!checkVar(index)) {
					throw new XmlParseException("ForEach index 不合法, 应是{xxx}");
				}
				index = getRealVal(index);
			}

			String open = StringUtils.trim(nodeToHandle.getStringAttribute("open"));
			String close = StringUtils.trim(nodeToHandle.getStringAttribute("close"));
			String separator = StringUtils.trim(nodeToHandle.getStringAttribute("separator"));

			List<TangYuanNode> contents = parseDynamicTags(nodeToHandle);
			int size = contents.size();
			TangYuanNode sqlNode = null;
			if (1 == size) {
				sqlNode = contents.get(0);
			} else if (size > 1) {
				sqlNode = new MixedNode(contents);
			}

			if (null == sqlNode && null == open && null == close && null == separator) {
				open = "(";
				close = ")";
				separator = ",";
			}

			if (null == sqlNode) {
				if (null == index) {
					index = "i";
				}
				// sqlNode = new StaticTextNode("#{" + collection + "[" + index + "]}");
				sqlNode = new SqlTextNode("#{" + collection + "[" + index + "]}");
			}

			// ForEachNode forEachNode = new SqlForEachNode(sqlNode, VariableParser.parse(collection, false), index, open, close, separator);
			ForEachNode forEachNode = new MongoForEachNode(sqlNode, new NormalParser().parse(collection), index, open, close, separator);
			targetContents.add(forEachNode);
		}
	}

	private class SetVarHandler implements NodeHandler {
		public void handleNode(XmlNodeWrapper nodeToHandle, List<TangYuanNode> targetContents) {
			// <setvar key="{x}" value="100" type="Integer" />
			String key = StringUtils.trim(nodeToHandle.getStringAttribute("key")); // xml
																					// validation
																					// 非空
			String _value = StringUtils.trim(nodeToHandle.getStringAttribute("value")); // xml
																						// validation
																						// 非空
			String type = StringUtils.trim(nodeToHandle.getStringAttribute("type")); // xml
																						// validation
			if (!checkVar(key)) {
				throw new XmlParseException("SetVar key 不合法, 应是{xxx}");
			}
			key = getRealVal(key);
			Object value = getSetVarValue(_value, type);
			SetVarNode setVarNode = new SetVarNode(key, value);
			targetContents.add(setVarNode);
		}
	}

	private Object getSetVarValue(String str, String type) {
		if (null == str) {
			return null;
		}
		if (null == type) {
			if ("true".equalsIgnoreCase(str) || "false".equalsIgnoreCase(str)) {
				return Boolean.parseBoolean(str);
			} else if (NumberUtils.isNumber(str)) {
				return NumberUtils.parseNumber(str);
			} else if (DateUtils.isDateTime(str)) {
				return DateUtils.parseDate(str);
			} else if (DateUtils.isOnlyDate(str)) {
				return DateUtils.parseSqlDate(str);
			} else if (DateUtils.isOnlyTime(str)) {
				return DateUtils.parseSqlTime(str);
			}
			// TODO: 时间类型的格式化末班需要在定义一些
			return str;
		} else {
			if ("int".equalsIgnoreCase(type) || "Integer".equalsIgnoreCase(type)) {
				return Integer.parseInt(str);
			} else if ("long".equalsIgnoreCase(type)) {
				return Long.parseLong(str);
			} else if ("float".equalsIgnoreCase(type)) {
				return Float.parseFloat(str);
			} else if ("double".equalsIgnoreCase(type)) {
				return Double.parseDouble(str);
			} else if ("short".equalsIgnoreCase(type)) {
				return Short.parseShort(str);
			} else if ("boolean".equalsIgnoreCase(type)) {
				return Boolean.parseBoolean(str);
			} else if ("byte".equalsIgnoreCase(type)) {
				return Byte.parseByte(str);
			} else if ("char".equalsIgnoreCase(type)) {
				return str.charAt(0);
			} else if ("dateTime".equalsIgnoreCase(type)) {
				return DateUtils.parseDate(str);
			} else if ("date".equalsIgnoreCase(type)) {
				return DateUtils.parseSqlDate(str);
			} else if ("time".equalsIgnoreCase(type)) {
				return DateUtils.parseSqlTime(str);
			}
			return str;
		}
	}

	private class LogHandler implements NodeHandler {
		public void handleNode(XmlNodeWrapper nodeToHandle, List<TangYuanNode> targetContents) {
			String message = StringUtils.trim(nodeToHandle.getStringAttribute("message")); // xml
																							// validation
			String _level = StringUtils.trim(nodeToHandle.getStringAttribute("level")); // xml
																						// validation
			int level = 3;
			if (null != _level) {
				level = getLogLevel(_level);
			}
			LogNode logNode = new LogNode(level, message);
			targetContents.add(logNode);
		}
	}

	private class ReturnHandler implements NodeHandler {
		public void handleNode(XmlNodeWrapper nodeToHandle, List<TangYuanNode> targetContents) {
			Variable result = null;
			String _result = StringUtils.trim(nodeToHandle.getStringAttribute("value"));
			if (null != _result) {
				if (!checkVar(_result)) {
					throw new XmlParseException("Return value 不合法, 应是{xxx}");
				}
				_result = getRealVal(_result);
				// result = VariableParser.parse(_result, false);
				result = new NormalParser().parse(_result);
			}

			List<ReturnItem> resultList = null;
			List<XmlNodeWrapper> properties = nodeToHandle.evalNodes("property");
			if (properties.size() > 0) {
				resultList = new ArrayList<ReturnItem>();
				for (XmlNodeWrapper propertyNode : properties) {
					String name = StringUtils.trim(propertyNode.getStringAttribute("name"));
					String value = StringUtils.trim(propertyNode.getStringAttribute("value"));
					if (!checkVar(value)) {
						throw new XmlParseException("Return property value 不合法, 应是{xxx}");
					}
					value = getRealVal(value);
					if (null != name) {
						if (!checkVar(name)) {
							throw new XmlParseException("Return property name 不合法, 应是{xxx}");
						}
						name = getRealVal(name);
					} else {
						name = value;
					}
					// resultList.add(new ReturnItem(name, VariableParser.parse(value, false)));
					resultList.add(new ReturnItem(name, new NormalParser().parse(value)));
				}
			}

			if (null != result && null != resultList) {
				throw new XmlParseException("Return节点中result|property只能选择一种方式");
			}

			ReturnNode returnNode = new ReturnNode(result, resultList, serviceResultType);
			targetContents.add(returnNode);
		}
	}

	private class ThrowHandler implements NodeHandler {
		public void handleNode(XmlNodeWrapper nodeToHandle, List<TangYuanNode> targetContents) {
			String test = StringUtils.trim(nodeToHandle.getStringAttribute("test")); // xml
																						// validation
			String code = StringUtils.trim(nodeToHandle.getStringAttribute("code")); // xml
																						// validation
			String message = StringUtils.trim(nodeToHandle.getStringAttribute("message"));
			String i18n = StringUtils.trim(nodeToHandle.getStringAttribute("i18n"));
			if (null == test || null == code) {
				throw new XmlParseException("Exception节点中test,code不能为空");
			}
			// log.info("ThrowHandler test:" + test);
			targetContents.add(new ExceptionNode(new LogicalExprParser().parse(test), Integer.parseInt(code), message, i18n));
		}
	}

	private class CallHandler implements NodeHandler {
		public void handleNode(XmlNodeWrapper nodeToHandle, List<TangYuanNode> targetContents) {
			String serviceId = StringUtils.trim(nodeToHandle.getStringAttribute("service"));
			if (null == serviceId) {
				throw new XmlParseException("The service attribute in the call node can not be empty");
			}

			// fix: 新增变量调用功能
			Object service = serviceId;
			if (checkVar(serviceId)) {
				service = new NormalParser().parse(serviceId);
			}

			String resultKey = getResultKey(StringUtils.trim(nodeToHandle.getStringAttribute("resultKey")));
			String _mode = StringUtils.trim(nodeToHandle.getStringAttribute("mode"));// xml v

			// CallMode mode = CallMode.EXTEND;
			CallMode mode = null;// 增加新的默认模式
			if (null != _mode) {
				mode = getCallMode(_mode);
			}
			String exResultKey = getResultKey(StringUtils.trim(nodeToHandle.getStringAttribute("exResultKey")));
			List<CallNodeParameterItem> itemList = null;

			List<XmlNodeWrapper> properties = nodeToHandle.evalNodes("property");
			if (properties.size() > 0) {
				itemList = new ArrayList<CallNodeParameterItem>();
				for (XmlNodeWrapper propertyNode : properties) {
					String name = StringUtils.trim(propertyNode.getStringAttribute("name"));
					String value = StringUtils.trim(propertyNode.getStringAttribute("value"));
					if (!checkVar(value)) {
						throw new XmlParseException("Call property value 不合法, 应是{xxx}");
					}
					value = getRealVal(value);
					if (null != name) {
						if (!checkVar(name)) {
							throw new XmlParseException("Call property name 不合法, 应是{xxx}");
						}
						name = getRealVal(name);
					} else {
						name = value;
					}
					// itemList.add(new CallNodeParameterItem(name, VariableParser.parse(value, false)));
					itemList.add(new CallNodeParameterItem(name, new NormalParser().parse(value)));
				}
			}

			// TODO:后面还需要做一个检测,是否有ID
			targetContents.add(new CallNode(service, resultKey, mode, itemList, exResultKey));
		}
	}

	private class SelectSetHandler implements NodeHandler {
		public void handleNode(XmlNodeWrapper nodeToHandle, List<TangYuanNode> targetContents) {
			TangYuanNode sqlNode = parseNode(nodeToHandle, true);
			if (null != sqlNode) {
				String dsKey = StringUtils.trim(nodeToHandle.getStringAttribute("dsKey"));
				if (null == dsKey) {
					dsKey = dsKeyWithSqlService;
				} else {
					if (!TangYuanMongoContainer.getInstance().getDataSourceManager().isValidDsKey(dsKey)) {
						throw new XmlParseException("无效的dsKey:" + dsKey);
					}
				}
				String resultKey = getResultKey(StringUtils.trim(nodeToHandle.getStringAttribute("resultKey")));
				Integer fetchSize = null;
				String _fetchSize = StringUtils.trim(nodeToHandle.getStringAttribute("fetchSize"));
				if (null != _fetchSize) {
					fetchSize = Integer.valueOf(_fetchSize);
				}

				String _cacheUse = StringUtils.trim(nodeToHandle.getStringAttribute("cacheUse"));
				CacheUseVo cacheUse = null;
				if (null != _cacheUse && _cacheUse.length() > 0) {
					cacheUse = parseCacheUse(_cacheUse, "");
				}

				InternalMongoSelectSetNode selectSetNode = new InternalMongoSelectSetNode(dsKey, resultKey, sqlNode, serviceResultType, fetchSize,
						cacheUse);
				targetContents.add(selectSetNode);
			}
		}
	}

	private class SelectOneHandler implements NodeHandler {
		public void handleNode(XmlNodeWrapper nodeToHandle, List<TangYuanNode> targetContents) {
			TangYuanNode sqlNode = parseNode(nodeToHandle, true);
			if (null != sqlNode) {
				String dsKey = StringUtils.trim(nodeToHandle.getStringAttribute("dsKey"));
				if (null == dsKey) {
					dsKey = dsKeyWithSqlService;
				} else {
					if (!TangYuanMongoContainer.getInstance().getDataSourceManager().isValidDsKey(dsKey)) {
						throw new XmlParseException("无效的dsKey:" + dsKey);
					}
				}
				String resultKey = getResultKey(StringUtils.trim(nodeToHandle.getStringAttribute("resultKey")));

				String _cacheUse = StringUtils.trim(nodeToHandle.getStringAttribute("cacheUse"));
				CacheUseVo cacheUse = null;
				if (null != _cacheUse && _cacheUse.length() > 0) {
					cacheUse = parseCacheUse(_cacheUse, "");
				}

				InternalMongoSelectOneNode selectOneNode = new InternalMongoSelectOneNode(dsKey, resultKey, sqlNode, serviceResultType, cacheUse);
				targetContents.add(selectOneNode);
			}
		}
	}

	private class SelectVarHandler implements NodeHandler {
		public void handleNode(XmlNodeWrapper nodeToHandle, List<TangYuanNode> targetContents) {
			TangYuanNode sqlNode = parseNode(nodeToHandle, true);
			if (null != sqlNode) {
				String dsKey = StringUtils.trim(nodeToHandle.getStringAttribute("dsKey"));
				if (null == dsKey) {
					dsKey = dsKeyWithSqlService;
				} else {
					if (!TangYuanMongoContainer.getInstance().getDataSourceManager().isValidDsKey(dsKey)) {
						throw new XmlParseException("无效的dsKey:" + dsKey);
					}
				}
				String resultKey = getResultKey(StringUtils.trim(nodeToHandle.getStringAttribute("resultKey")));

				String _cacheUse = StringUtils.trim(nodeToHandle.getStringAttribute("cacheUse"));
				CacheUseVo cacheUse = null;
				if (null != _cacheUse && _cacheUse.length() > 0) {
					cacheUse = parseCacheUse(_cacheUse, "");
				}

				InternalMongoSelectVarNode selectVarNode = new InternalMongoSelectVarNode(dsKey, resultKey, sqlNode, cacheUse);
				targetContents.add(selectVarNode);
			}
		}
	}

	private class DeleteHandler implements NodeHandler {
		public void handleNode(XmlNodeWrapper nodeToHandle, List<TangYuanNode> targetContents) {
			TangYuanNode sqlNode = parseNode(nodeToHandle, true);
			if (null != sqlNode) {
				String dsKey = StringUtils.trim(nodeToHandle.getStringAttribute("dsKey"));
				if (null == dsKey) {
					dsKey = dsKeyWithSqlService;
				} else {
					if (!TangYuanMongoContainer.getInstance().getDataSourceManager().isValidDsKey(dsKey)) {
						throw new XmlParseException("无效的dsKey:" + dsKey);
					}
				}
				String resultKey = StringUtils.trim(nodeToHandle.getStringAttribute("rowCount"));
				if (null != resultKey) {
					if (!checkVar(resultKey)) {
						throw new XmlParseException("Delete rowCount 不合法, 应是{xxx}");
					}
					resultKey = getRealVal(resultKey);
				}

				String _cacheClean = StringUtils.trim(nodeToHandle.getStringAttribute("cacheClean"));
				CacheCleanVo cacheClean = null;
				if (null != _cacheClean && _cacheClean.length() > 0) {
					cacheClean = parseCacheClean(_cacheClean, "");
				}

				InternalMongoDeleteNode deleteNode = new InternalMongoDeleteNode(dsKey, resultKey, sqlNode, cacheClean);
				targetContents.add(deleteNode);
			}
		}
	}

	private class UpdateHandler implements NodeHandler {
		public void handleNode(XmlNodeWrapper nodeToHandle, List<TangYuanNode> targetContents) {
			TangYuanNode sqlNode = parseNode(nodeToHandle, true);
			if (null != sqlNode) {
				String dsKey = StringUtils.trim(nodeToHandle.getStringAttribute("dsKey"));
				if (null == dsKey) {
					dsKey = dsKeyWithSqlService;
				} else {
					if (!TangYuanMongoContainer.getInstance().getDataSourceManager().isValidDsKey(dsKey)) {
						throw new XmlParseException("无效的dsKey:" + dsKey);
					}
				}
				String resultKey = StringUtils.trim(nodeToHandle.getStringAttribute("rowCount"));
				if (null != resultKey) {
					if (!checkVar(resultKey)) {
						throw new XmlParseException("Update rowCount 不合法, 应是{xxx}");
					}
					resultKey = getRealVal(resultKey);
				}

				String _cacheClean = StringUtils.trim(nodeToHandle.getStringAttribute("cacheClean"));
				CacheCleanVo cacheClean = null;
				if (null != _cacheClean && _cacheClean.length() > 0) {
					cacheClean = parseCacheClean(_cacheClean, "");
				}

				InternalMongoUpdateNode updateNode = new InternalMongoUpdateNode(dsKey, resultKey, sqlNode, cacheClean);
				targetContents.add(updateNode);
			}
		}
	}

	private class InsertHandler implements NodeHandler {
		public void handleNode(XmlNodeWrapper nodeToHandle, List<TangYuanNode> targetContents) {
			TangYuanNode sqlNode = parseNode(nodeToHandle, true);
			if (null != sqlNode) {
				String dsKey = StringUtils.trim(nodeToHandle.getStringAttribute("dsKey"));
				if (null == dsKey) {
					dsKey = dsKeyWithSqlService;
				} else {
					if (!TangYuanMongoContainer.getInstance().getDataSourceManager().isValidDsKey(dsKey)) {
						throw new XmlParseException("无效的dsKey:" + dsKey);
					}
				}

				// String resultKey = StringUtils.trim(nodeToHandle.getStringAttribute("rowCount"));
				// if (null != resultKey) {
				// if (!checkVar(resultKey)) {
				// throw new XmlParseException("Insert rowCount 不合法, 应是{xxx}");
				// }
				// resultKey = getRealVal(resultKey);
				// }

				String resultKey = StringUtils.trim(nodeToHandle.getStringAttribute("resultKey"));
				if (null != resultKey) {
					if (!checkVar(resultKey)) {
						throw new XmlParseException("Insert resultKey 不合法, 应是{xxx}");
					}
					resultKey = getRealVal(resultKey);
				}

				// String incrementKey = StringUtils.trim(nodeToHandle.getStringAttribute("incrementKey"));
				// if (null != incrementKey) {
				// if (!checkVar(incrementKey)) {
				// throw new XmlParseException("Insert incrementKey 不合法, 应是{xxx}");
				// }
				// incrementKey = getRealVal(incrementKey);
				// }

				String _cacheClean = StringUtils.trim(nodeToHandle.getStringAttribute("cacheClean"));
				CacheCleanVo cacheClean = null;
				if (null != _cacheClean && _cacheClean.length() > 0) {
					cacheClean = parseCacheClean(_cacheClean, "");
				}

				// InternalMongoInsertNode insertNode = new InternalMongoInsertNode(dsKey, resultKey, incrementKey, sqlNode, cacheClean);

				InternalMongoInsertNode insertNode = new InternalMongoInsertNode(dsKey, resultKey, sqlNode, cacheClean);

				targetContents.add(insertNode);
			}
		}
	}

	private int getLogLevel(String str) {
		if ("ERROR".equalsIgnoreCase(str)) {
			return 5;
		} else if ("WARN".equalsIgnoreCase(str)) {
			return 4;
		} else if ("INFO".equalsIgnoreCase(str)) {
			return 3;
		} else if ("DEBUG".equalsIgnoreCase(str)) {
			return 2;
		} else {
			return 1;
		}
	}

	private CallMode getCallMode(String str) {
		if ("EXTEND".equalsIgnoreCase(str)) {
			return CallMode.EXTEND;
		} else if ("ALONE".equalsIgnoreCase(str)) {
			return CallMode.ALONE;
		} else if ("ASYNC".equalsIgnoreCase(str)) {
			return CallMode.ASYNC;
		} else {
			return CallMode.EXTEND;
		}
	}

	private Map<String, NodeHandler> nodeHandlers = new HashMap<String, NodeHandler>() {
		private static final long serialVersionUID = 1L;

		{
			put("foreach", new ForEachHandler());
			put("if", new IfHandler());
			put("else", new ElseHandler());
			put("elseif", new ElseIfHandler());
			put("include", new IncludeHandler());
			put("exception", new ThrowHandler());
			put("return", new ReturnHandler());
			put("setvar", new SetVarHandler());
			put("log", new LogHandler());
			put("selectSet", new SelectSetHandler());
			put("selectOne", new SelectOneHandler());
			put("selectVar", new SelectVarHandler());
			put("update", new UpdateHandler());
			put("delete", new DeleteHandler());
			put("insert", new InsertHandler());
			put("call", new CallHandler());
		}
	};

}
