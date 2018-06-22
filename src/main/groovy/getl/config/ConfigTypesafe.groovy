package getl.config

import com.typesafe.config.ConfigFactory
import getl.exception.ExceptionGETL
import getl.utils.Config as GETLConfig
import com.typesafe.config.Config as TypeSafeConfig
import getl.utils.Logs

/**
 * Typesafe Config wrapper
 * @author Dmitry Shaldin
 *
 */
class ConfigTypesafe extends ConfigManager {
	public String getPath() {
		return GETLConfig.params.path as String
	}

	public void setPath(String value) {
		GETLConfig.params.path = value
	}

	@Override
	void loadConfig(Map<String, Object> readParams) {
		String path = (readParams.path) ?: path
		TypeSafeConfig config = path ? ConfigFactory.load(path) : ConfigFactory.load()
		GETLConfig.MergeConfig(config.root().unwrapped())
	}

	@Override
	void saveConfig(Map<String, Object> content, Map<String, Object> saveParams) {
		throw new ExceptionGETL('Not supported')
	}

	@Override
	void init(Map<String, Object> initParams) {
		if (initParams?.config == null) return
		Map config = initParams.config as Map<String, Object>
		if (config.path != null) {
			this.path = config.path
			Logs.Config("config: set path ${path}")
		}
	}
}
