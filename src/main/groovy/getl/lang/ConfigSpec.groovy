/*
 GETL - based package in Groovy, which automates the work of loading and transforming data. His name is an acronym for "Groovy ETL".

 GETL is a set of libraries of pre-built classes and objects that can be used to solve problems unpacking,
 transform and load data into programs written in Groovy, or Java, as well as from any software that supports
 the work with Java classes.

 Copyright (C) 2013-2019  Alexsey Konstantonov (ASCRUS)

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

package getl.lang

import getl.config.*
import getl.utils.*

/**
 * Config specification class
 * @author Alexsey Konstantinov
 *
 */
class ConfigSpec {
    /**
     * Configuration manager
     */
    ConfigSlurper getManager() { Config.configClassManager as ConfigSlurper}

    /**
     * Configuration files path
     */
    String getPath() { manager.path }
    void setPath(String value) { manager.path = value }

    /**
     * Code page in configuration files
     */
    String getCodePage() { manager.codePage }
    void setCodePage(String value) { manager.codePage = value }

    /**
     * Load configuration file
     * @param fileName
     * @param codePage
     * @return
     */
    void load(String fileName, String codePage = null) { Config.LoadConfig(fileName: fileName, codePage: codePage) }

    /**
     * Save configuration file
     * @param fileName
     * @param codePage
     * @return
     */
    void save(String fileName, String codePage = null) { Config.SaveConfig(fileName: fileName, codePage: codePage)}

    void clear() { Config.ClearConfig() }
}