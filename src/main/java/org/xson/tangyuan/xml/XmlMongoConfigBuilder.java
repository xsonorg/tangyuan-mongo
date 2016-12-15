package org.xson.tangyuan.xml;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.xson.tangyuan.TangYuanContainer;
import org.xson.tangyuan.TangYuanMongoContainer;
import org.xson.tangyuan.datasource.DataSourceException;
import org.xson.tangyuan.datasource.MongoDataSource;
import org.xson.tangyuan.datasource.MongoDataSourceGroupVo;
import org.xson.tangyuan.datasource.MongoDataSourceManager;
import org.xson.tangyuan.datasource.MongoDataSourceVo;
import org.xson.tangyuan.datasource.MongoSupport;
import org.xson.tangyuan.datasource.MuiltMongoDataSourceManager;
import org.xson.tangyuan.datasource.SimpleMongoDataSourceManager;
import org.xson.tangyuan.executor.MongoServiceContextFactory;
import org.xson.tangyuan.logging.Log;
import org.xson.tangyuan.logging.LogFactory;
import org.xson.tangyuan.transaction.DefaultTransactionMatcher;
import org.xson.tangyuan.util.Resources;
import org.xson.tangyuan.util.StringUtils;
import org.xson.tangyuan.xml.node.AbstractServiceNode.TangYuanServiceType;
import org.xson.tangyuan.xml.node.TangYuanNode;
import org.xson.tangyuan.xml.node.XMLMongoNodeBuilder;

public class XmlMongoConfigBuilder implements XmlExtendBuilder {

	private Log								log						= LogFactory.getLog(getClass());
	private XPathParser						xPathParser				= null;
	private DefaultTransactionMatcher		transactionMatcher		= new DefaultTransactionMatcher();
	private Map<String, MongoDataSourceVo>	dataSourceVoMap			= new HashMap<String, MongoDataSourceVo>();

	private String							defaultDataSource		= null;
	private XmlMongoMapperBuilder			xmlMapperBuilder		= null;
	private XmlMongoShardingBuilder			xmlShardingBuilder		= null;
	private List<XMLMongoNodeBuilder>		xmlsqlNodeBuilderList	= null;
	private XmlConfigurationBuilder			xmlConfigurationBuilder	= null;

	public void parse(XmlConfigurationBuilder xmlConfigurationBuilder, String resource) throws Throwable {
		this.xmlConfigurationBuilder = xmlConfigurationBuilder;
		InputStream inputStream = Resources.getResourceAsStream(resource);
		this.xPathParser = new XPathParser(inputStream);
		log.info("Start parsing: " + resource);
		configurationElement(xPathParser.evalNode("/configuration"));
	}

	private void configurationElement(XmlNodeWrapper context) {
		try {
			// 先解析默认设置
			List<MongoDataSourceVo> dsList = buildDataSourceNodes(context.evalNodes("dataSource"));// 解析dataSource
			List<MongoDataSourceVo> dsGroupList = buildDataSourceGroupNodes(context.evalNodes("dataSourceGroup"));// 解析dataSourceGroup
			addDataSource(dsList, dsGroupList);

			buildMapperNodes(context.evalNodes("mapper"));
			buildShardingNodes(context.evalNodes("sharding"));
			buildPluginNodes(context.evalNodes("plugin"));
			// 注册所需
			after();
		} catch (Exception e) {
			throw new XmlParseException(e);
		}
	}

	private List<MongoDataSourceVo> buildDataSourceNodes(List<XmlNodeWrapper> contexts) {
		int size = contexts.size();
		List<MongoDataSourceVo> dsList = new ArrayList<MongoDataSourceVo>();
		for (int i = 0; i < size; i++) {
			XmlNodeWrapper xNode = contexts.get(i);
			String id = StringUtils.trim(xNode.getStringAttribute("id"));
			if (dataSourceVoMap.containsKey(id)) {
				throw new XmlParseException("重复的数据源ID: " + id);
			}
			String tmp = null;
			// String tmp = StringUtils.trim(xNode.getStringAttribute("type"));
			// ConnPoolType type = null;
			// TODO type
			// if (null == type) {
			// throw new XmlParseException("无效的数据源类型");
			// }
			boolean defaultDs = false;
			tmp = StringUtils.trim(xNode.getStringAttribute("isDefault"));
			if (null != tmp) {
				if ("true".equalsIgnoreCase(tmp)) {
					if (null != this.defaultDataSource) {
						throw new XmlParseException("The default mongo data source must be unique.");
					} else {
						this.defaultDataSource = id;
					}
					defaultDs = true;
				}
			}
			Map<String, String> data = new HashMap<String, String>();
			List<XmlNodeWrapper> properties = xNode.evalNodes("property");
			for (XmlNodeWrapper propertyNode : properties) {
				data.put(StringUtils.trim(propertyNode.getStringAttribute("name")).toUpperCase(),
						StringUtils.trim(propertyNode.getStringAttribute("value")));
			}
			MongoDataSourceVo dsVo = new MongoDataSourceVo(id, data, defaultDs);
			dsList.add(dsVo);
			dataSourceVoMap.put(id, dsVo);
		}
		return dsList;
	}

	private List<MongoDataSourceVo> buildDataSourceGroupNodes(List<XmlNodeWrapper> contexts) {
		// log.info("解析数据源:" + contexts.size());
		int size = contexts.size();
		List<MongoDataSourceVo> dsList = new ArrayList<MongoDataSourceVo>();
		for (int i = 0; i < size; i++) {
			XmlNodeWrapper xNode = contexts.get(i);
			String id = StringUtils.trim(xNode.getStringAttribute("groupId"));
			if (dataSourceVoMap.containsKey(id)) {
				throw new XmlParseException("Duplicate mongo data source: " + id);
			}
			String tmp = null;
			// String tmp = StringUtils.trim(xNode.getStringAttribute("type"));
			// ConnPoolType type = null;
			// ConnPoolType type = getConnPoolType(tmp);
			// if (null == type) {
			// throw new XmlParseException("无效的数据源类型");
			// }
			tmp = StringUtils.trim(xNode.getStringAttribute("start")); // xml
																		// validation
			int start = 0;
			if (null != tmp) {
				start = Integer.parseInt(tmp);
			}
			tmp = StringUtils.trim(xNode.getStringAttribute("end")); // xml
																		// validation
			int end = Integer.parseInt(tmp);
			boolean defaultDs = false;
			tmp = StringUtils.trim(xNode.getStringAttribute("isDefault"));
			if (null != tmp) {
				if ("true".equalsIgnoreCase(tmp)) {
					if (null != this.defaultDataSource) {
						throw new XmlParseException("The default mongo data source must be unique.");
					} else {
						this.defaultDataSource = id;
					}
					defaultDs = true;
				}
			}

			Map<String, String> data = new HashMap<String, String>();
			List<XmlNodeWrapper> properties = xNode.evalNodes("property");
			for (XmlNodeWrapper propertyNode : properties) {
				data.put(StringUtils.trim(propertyNode.getStringAttribute("name")).toUpperCase(),
						StringUtils.trim(propertyNode.getStringAttribute("value")));
			}
			MongoDataSourceGroupVo dsGroupVo = new MongoDataSourceGroupVo(id, defaultDs, data, start, end);
			dsList.add(dsGroupVo);

			dataSourceVoMap.put(id, dsGroupVo);
		}
		return dsList;
	}

	private void addDataSource(List<MongoDataSourceVo> dsList, List<MongoDataSourceVo> dsGroupList) throws Exception {
		List<MongoDataSourceVo> allList = new ArrayList<MongoDataSourceVo>();
		if (null != dsList && dsList.size() > 0) {
			allList.addAll(dsList);
		}
		if (null != dsGroupList && dsGroupList.size() > 0) {
			allList.addAll(dsGroupList);
		}
		if (0 == allList.size()) {
			throw new XmlParseException("mongo data source is missing.");
		}

		// Map<String, String> decryptProperties = null;
		MongoDataSourceManager dataSourceManager = null;

		// 简单唯一数据源
		if (1 == allList.size() && !allList.get(0).isGroup()) {
			MongoDataSourceVo dsVo = allList.get(0);
			// AbstractDataSource dataSource = new DataSourceCreater().create(dsVo, decryptProperties);
			MongoDataSource dataSource = new MongoDataSource();
			dataSource.init(dsVo.getProperties());
			// dataSourceManager = new SimpleMongoDataSourceManager(dataSource, dsVo.getId());
			dataSourceManager = new SimpleMongoDataSourceManager(dataSource, dsVo.getId());
			log.info("add mongo datasource: " + dsVo.getId());
		} else {

			Map<String, MongoDataSourceVo> logicDataSourceMap = new HashMap<String, MongoDataSourceVo>();
			Map<String, MongoDataSource> realDataSourceMap = new HashMap<String, MongoDataSource>();
			// String _defaultDsKey = null;

			for (MongoDataSourceVo dsVo : allList) {
				if (dsVo.isGroup()) {
					// new DataSourceCreater().create((MongoDataSourceGroupVo) dsVo, realDataSourceMap, decryptProperties);
					MongoDataSourceGroupVo dsGroupVo = (MongoDataSourceGroupVo) dsVo;
					Map<String, String> properties = dsVo.getProperties();
					for (int i = dsGroupVo.getStart(); i <= dsGroupVo.getEnd(); i++) {
						// BasicDataSource dsPool = createDataSource(properties, decryptProperties, i + "");
						if (realDataSourceMap.containsKey(dsGroupVo.getId() + "." + i)) {
							throw new DataSourceException("Duplicate mongo data source: " + dsGroupVo.getId() + "." + i);
						}

						MongoDataSource dataSource = new MongoDataSource();
						dataSource.init(properties);

						realDataSourceMap.put(dsGroupVo.getId() + "." + i, dataSource);
						log.info("add mongo group datasource: " + dsGroupVo.getId() + "." + i);
					}
				} else {
					MongoDataSource dataSource = new MongoDataSource();
					dataSource.init(dsVo.getProperties());
					if (realDataSourceMap.containsKey(dsVo.getId())) {
						throw new DataSourceException("Duplicate mongo data source: " + dsVo.getId());
					}
					realDataSourceMap.put(dsVo.getId(), dataSource);
				}
				if (logicDataSourceMap.containsKey(dsVo.getId())) {
					throw new DataSourceException("Duplicate mongo data source: " + dsVo.getId());
				}
				logicDataSourceMap.put(dsVo.getId(), dsVo);
				// if (dsVo.isDefaultDs()) {
				// _defaultDsKey = dsVo.getId();
				// }
				log.info("add datasource: " + dsVo.getId());
			}
			dataSourceManager = new MuiltMongoDataSourceManager(logicDataSourceMap, realDataSourceMap);
		}
		TangYuanMongoContainer.getInstance().setDataSourceManager(dataSourceManager);
		MongoSupport.setManager(dataSourceManager);
	}

	/**
	 * 解析mapper
	 */
	private void buildMapperNodes(List<XmlNodeWrapper> contexts) throws Exception {
		int size = contexts.size();
		if (size == 0) {
			return;
		}
		if (size > 1) {
			throw new XmlParseException("mapper只能有一项");
		}
		XmlNodeWrapper xNode = contexts.get(0);
		String resource = StringUtils.trim(xNode.getStringAttribute("resource")); // xml
																					// v
		log.info("Start parsing: " + resource);
		InputStream inputStream = Resources.getResourceAsStream(resource);
		xmlMapperBuilder = new XmlMongoMapperBuilder(inputStream);
		xmlMapperBuilder.parse();
	}

	/**
	 * 解析Sharding
	 */
	private void buildShardingNodes(List<XmlNodeWrapper> contexts) throws Exception {
		int size = contexts.size();
		if (size == 0) {
			return;
		}
		if (size > 1) {
			throw new XmlParseException("sharding只能有一项");
		}
		XmlNodeWrapper xNode = contexts.get(0);
		String resource = StringUtils.trim(xNode.getStringAttribute("resource")); // xml
		// v
		log.info("Start parsing: " + resource);
		InputStream inputStream = Resources.getResourceAsStream(resource);
		xmlShardingBuilder = new XmlMongoShardingBuilder(inputStream, dataSourceVoMap);
		xmlShardingBuilder.parse();
	}

	private void buildPluginNodes(List<XmlNodeWrapper> contexts) throws Exception {
		int size = contexts.size();
		if (size == 0) {
			return;
		}
		List<String> resourceList = new ArrayList<String>();
		xmlsqlNodeBuilderList = new ArrayList<XMLMongoNodeBuilder>();

		// 防止重复, 全局控制
		Map<String, TangYuanNode> integralRefMap = new HashMap<String, TangYuanNode>();
		Map<String, Integer> integralServiceMap = new HashMap<String, Integer>();

		// 扫描所有的<SQL>
		for (int i = 0; i < size; i++) {
			XmlNodeWrapper xNode = contexts.get(i);
			String resource = StringUtils.trim(xNode.getStringAttribute("resource"));
			log.info("Start parsing: " + resource);
			InputStream inputStream = Resources.getResourceAsStream(resource);
			XMLMongoNodeBuilder xmlSqlNodeBuilder = new XMLMongoNodeBuilder(inputStream, this, xmlConfigurationBuilder, xmlMapperBuilder,
					integralRefMap, integralServiceMap);
			xmlSqlNodeBuilder.parseRef();
			xmlsqlNodeBuilderList.add(xmlSqlNodeBuilder);
			resourceList.add(resource);
		}

		// 注册所有的服务
		for (int i = 0; i < size; i++) {
			log.info("Start parsing: " + resourceList.get(i));
			xmlsqlNodeBuilderList.get(i).parseService();
		}
	}

	public void after() {
		TangYuanContainer.getInstance().registerContextFactory(TangYuanServiceType.MONGO, new MongoServiceContextFactory());
	}

	public DefaultTransactionMatcher getTransactionMatcher() {
		return transactionMatcher;
	}

	public Map<String, MongoDataSourceVo> getDataSourceVoMap() {
		return dataSourceVoMap;
	}

	public String getDefaultDsKey() {
		return defaultDataSource;
	}

}
