/*
 GETL - based package in Groovy, which automates the work of loading and transforming data. His name is an acronym for "Groovy ETL".

 GETL is a set of libraries of pre-built classes and objects that can be used to solve problems unpacking,
 transform and load data into programs written in Groovy, or Java, as well as from any software that supports
 the work with Java classes.

 Copyright (C) EasyData Company LTD

 This program is free software: you can redistribute it and/or modify
 it under the terms of the GNU Lesser General Public License as published by
 the Free Software Foundation, either version 3 of the License, or
 (at your option) any later version.

 This program is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU General Public License for more details.

 You should have received a copy of the GNU General Public License and
 GNU Lesser General Public License along with this program.
 If not, see <http://www.gnu.org/licenses/>.
*/

package getl.config

import com.typesafe.config.ConfigFactory
import getl.exception.ExceptionGETL
import getl.utils.Config as GETLConfig
import com.typesafe.config.Config as TypeSafeConfig
import getl.utils.Logs

/**
 * Configuration typesafe config wrapper manager
 * @author Dmitry Shaldin
 *
 */
class ConfigTypesafe extends ConfigManager {
	String getPath() {
		return params.path as String
	}

	void setPath(String value) {
		params.path = value
	}

	@Override
	void loadConfig(Map<String, Object> readParams) {
		String path = (readParams.path) ?: path
		TypeSafeConfig config = path ? ConfigFactory.load(path) : ConfigFactory.load()
		GETLConfig.MergeConfig(config.root().unwrapped())
	}

	@Override
	void saveConfig(Map<String, Object> content, Map<String, Object> saveParams) {
		throw new ExceptionGETL('Not support this features!')
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
