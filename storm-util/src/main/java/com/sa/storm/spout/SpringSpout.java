/**
 * 
 */
package com.sa.storm.spout;

import org.springframework.context.ApplicationContext;

import com.sa.storm.framework.App;

/**
 * @author lewis
 * 
 */
public abstract class SpringSpout extends BaseSpout {

	private static final long serialVersionUID = 1L;

	private transient ApplicationContext springContext;

	/**
	 * @return the springContext
	 */
	protected ApplicationContext getSpringContext() {
		if (springContext == null) {
			springContext = App.getInstance().getContext();
		}

		return springContext;
	}

}
