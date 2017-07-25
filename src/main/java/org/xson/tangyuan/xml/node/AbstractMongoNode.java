package org.xson.tangyuan.xml.node;

public abstract class AbstractMongoNode extends AbstractServiceNode {

	protected TangYuanNode	sqlNode;

	protected String		dsKey;

	protected boolean		simple;

	protected AbstractMongoNode() {
		this.serviceType = TangYuanServiceType.MONGO;
	}

	public String getDsKey() {
		return dsKey;
	}

	public boolean isSimple() {
		return simple;
	}
}
