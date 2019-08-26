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

import getl.exception.ExceptionGETL
import getl.utils.Config
import getl.utils.FileUtils
import getl.utils.Logs
import getl.utils.MapUtils
import groovy.json.JsonParserType
import groovy.json.JsonSlurper
import org.h2.mvstore.MVMap
import org.h2.mvstore.MVStore

/**
 * Configuration security store manager class
 * @author Alexsey Konstantinov
 *
 */
class ConfigStores extends ConfigManager {
    @Override
    boolean getEvalVars() { true }

    @Override
    void init(Map<String, Object> initParams) {
        if (initParams?.config == null) return
        Map config = initParams.config as Map<String, Object>
        if (config.filename != null) {
            def fn = config.filename as String
            this.fileName = fn
            Logs.Config("config: use file ${this.fileName}")
        }

        if (config.section != null) {
            this.section = config.section
            Logs.Config("config: use section \"${this.fileName}\"")
        }
    }

    /**
     * Configuration file name
     */
    String getFileName () { params.fileName as String }

    /**
     * Set configuration file name
     * @param value
     */
    void setFileName (String value) {
        if (value == null || value.trim() == '') throw new ExceptionGETL('The file name can not have empty value')
        params.fileName = value.trim()
    }

    /**
     * Configuration section name
     */
    String getSection () { (params.section as String)?:'config' }

    /**
     * Set configuration section name
     * @param value
     */
    void setSection (String value) {
        if (value == null || value.trim() == '') throw new ExceptionGETL('The section name can not have empty value')
        params.section = value.trim()
    }

    /**
     * Configuration secret key
     */
    String getSecretKey () { (params.secretKey as String) }

    /**
     * Set configuration secret key
     * @param value
     */
    void setSecretKey (String value) {
        if (value == '') throw new ExceptionGETL('The secret key can not have empty value')
        params.secretKey = value
    }

    @Override
    void loadConfig(Map<String, Object> readParams) {
        def fileName = (readParams?.fileName as String)?:this.fileName
        def section = (readParams?.section as String)?:this.section
        def secretKey = (readParams?.secretKey as String)?:this.secretKey

        if (fileName == null) return

        Map<String, Object> data = LoadSection(fileName, secretKey, section)
        Config.MergeConfig(data)
    }

    static Map<String, Object> LoadSection(String fileName, String secretKey, String section) {
        if (fileName == null) throw new ExceptionGETL('Required fileName parameter')

        Map<String, Object> data = [:]

        if (FileUtils.FileExtension(fileName) == '') fileName += '.store'
        if (!FileUtils.ExistsFile(fileName)) throw new ExceptionGETL("Can not find store config file \"$fileName\"")

        MVStore store = new MVStore.Builder().fileName(fileName).encryptionKey((secretKey?:(this.getClass().name)).toCharArray()).compress().open()
        try {
            MVMap<String, HashMap<String, Object>> map = store.openMap(section)
            data.putAll(map)
        }
        finally {
            store.close()
        }

        return data
    }

    @Override
    void saveConfig(Map<String, Object> content, Map<String, Object> saveParams) {
        def fileName = (saveParams?.fileName as String)?:this.fileName
        def section = (saveParams?.section as String)?:this.section
        def secretKey = (saveParams?.secretKey as String)?:this.secretKey

        SaveSection(content, fileName, secretKey, section)
    }

    static void SaveSection(Map<String, Object> data, String fileName, String secretKey, String section) {
        def text = MapUtils.ToJson(data)
        def json = new JsonSlurper()
        data = MapUtils.Lazy2HashMap(json.parseText(text))

        if (FileUtils.FileExtension(fileName) == '') fileName += '.store'
        FileUtils.ValidFilePath(fileName)

        MVStore store = new MVStore.Builder().fileName(fileName).encryptionKey((secretKey?:(this.getClass().name)).toCharArray()).compress().open()
        try {
            MVMap<String, HashMap<String, Object>> map = store.openMap(section)
            map.putAll(data)
            store.commit()
        }
        finally {
            store.close()
        }
    }
}