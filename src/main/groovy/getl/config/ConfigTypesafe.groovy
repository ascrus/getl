package getl.config

import com.typesafe.config.ConfigFactory
import com.typesafe.config.Config as TypeSafeConfig
import getl.exception.NotSupportError
import groovy.transform.InheritConstructors

/**
 * Configuration typesafe config wrapper manager
 * @author Dmitry Shaldin
 *
 */
@InheritConstructors
class ConfigTypesafe extends ConfigManager {
	/** Path to configuration files */
	String getPath() {
		return params.path as String
	}
	/** Path to configuration files */
	void setPath(String value) {
		params.path = value
	}

	@Override
	void setEvalVars(Boolean value) { super.setEvalVars(false) }

	@Override
	protected void loadContent(Map<String, Object> readParams = new HashMap<String, Object>()) {
		String path = (readParams.path) ?: path
		TypeSafeConfig config = path ? ConfigFactory.load(path) : ConfigFactory.load()
		mergeConfig(config.root().unwrapped())
	}

	@Override
	void saveConfig(Map<String, Object> content, Map<String, Object> saveParams = new HashMap<String, Object>()) {
		throw new NotSupportError('saveConfig')
	}

	@Override
	void init(Map<String, Object> initParams) {
		super.init(initParams)
		evalVars = false

		if (initParams?.config == null)
			return

		Map config = initParams.config as Map<String, Object>
		if (config.path != null) {
			this.path = config.path
			logger.config("config: set path ${path}")
		}
	}
}
