package org.xson.tangyuan.executor;

public class MongoServiceContextFactory implements ServiceContextFactory {

	@Override
	public IServiceContext create() {
		return new MongoServiceContext();
	}

}
