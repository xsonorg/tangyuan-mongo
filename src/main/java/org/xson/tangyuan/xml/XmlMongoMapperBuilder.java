package org.xson.tangyuan.xml;

import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.xson.tangyuan.logging.Log;
import org.xson.tangyuan.logging.LogFactory;
import org.xson.tangyuan.mapping.MappingHandler;
import org.xson.tangyuan.mapping.MappingVo;
import org.xson.tangyuan.util.ClassUtils;
import org.xson.tangyuan.util.StringUtils;

public class XmlMongoMapperBuilder {

	private Log							log				= LogFactory.getLog(getClass());
	private XPathParser					xPathParser		= null;
	private Map<String, MappingHandler>	mappingClassMap	= new HashMap<String, MappingHandler>();
	private Map<String, MappingVo>		mappingVoMap	= new HashMap<String, MappingVo>();

	public XmlMongoMapperBuilder(InputStream inputStream) {
		this.xPathParser = new XPathParser(inputStream);
	}

	public void parse() {
		configurationElement(xPathParser.evalNode("/mapper"));
	}

	private void configurationElement(XmlNodeWrapper context) {
		try {
			// buildDataTypeMappingNodes(context.evalNodes("dataTypeMapping"));
			buildMappingClassNodes(context.evalNodes("mappingClass"));
			buildResultMapNodes(context.evalNodes("resultMap"));
		} catch (Exception e) {
			throw new XmlParseException(e);
		}
	}

	private void buildMappingClassNodes(List<XmlNodeWrapper> contexts) throws Exception {
		int size = contexts.size();
		for (int i = 0; i < size; i++) {
			XmlNodeWrapper xNode = contexts.get(i);
			String id = StringUtils.trim(xNode.getStringAttribute("id"));
			if (null != mappingClassMap.get(id)) {
				throw new XmlParseException("Duplicate mappingClass id: " + id);
			}
			String className = StringUtils.trim(xNode.getStringAttribute("class"));
			Class<?> handlerClass = ClassUtils.forName(className);
			if (!MappingHandler.class.isAssignableFrom(handlerClass)) {
				throw new XmlParseException("mapping class not implement the MappingHandler interface: " + className);
			}
			MappingHandler handler = (MappingHandler) handlerClass.newInstance();
			mappingClassMap.put(id, handler);
			log.info("add mapping handler: " + className);
		}
	}

	private void buildResultMapNodes(List<XmlNodeWrapper> contexts) {
		int size = contexts.size();
		for (int i = 0; i < size; i++) {
			XmlNodeWrapper xNode = contexts.get(i);
			String id = StringUtils.trim(xNode.getStringAttribute("id"));// xmlV
			if (mappingVoMap.containsKey(id)) {
				throw new XmlParseException("重复的mapping:" + id);
			}
			String type = StringUtils.trim(xNode.getStringAttribute("type"));
			Class<?> beanClass = null;

			if (null == type) {
				// beanClass = Map.class;// 这里需要看resultType
			} else if (!"*".equals(type)) {
				beanClass = ClassUtils.forName(type);
			}

			String rule = StringUtils.trim(xNode.getStringAttribute("rule"));
			MappingHandler handler = mappingClassMap.get(rule);

			Map<String, String> columnMap = new HashMap<String, String>();

			List<XmlNodeWrapper> properties = xNode.evalNodes("result");
			for (XmlNodeWrapper propertyNode : properties) {
				columnMap.put(StringUtils.trim(propertyNode.getStringAttribute("column")),
						StringUtils.trim(propertyNode.getStringAttribute("property")));
			}

			if (null == handler && 0 == columnMap.size()) {
				throw new XmlParseException("handler和columnMap 不能都为空:" + id);
			}

			MappingVo mVo = new MappingVo(id, type, beanClass, handler, columnMap);
			mappingVoMap.put(id, mVo);
			log.info("add resultMap: " + id);
		}
	}

	public Map<String, MappingVo> getMappingVoMap() {
		return mappingVoMap;
	}

}
