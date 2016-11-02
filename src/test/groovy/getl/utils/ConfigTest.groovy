package getl.utils

import getl.tfs.TFS
import groovy.json.JsonBuilder

/**
 * Created by ascru on 01.11.2016.
 */
class ConfigTest extends GroovyTestCase {
    void testSaveLoadConfig() {
        def configPath = new TFS()
        def configFile = new File("${configPath.path}/test_config.conf")

        def builder = new JsonBuilder()
        def conf = builder.root {
            map {
                list 'a', 1, null
            }
            var '${test_var}'
        }
        Config.content.putAll(conf.root)
        Config.SaveConfig(configFile)
        assertTrue(configFile.exists())
        println "FILE: ${configFile.text}"

        Config.ClearConfig()
        assertTrue(MapUtils.CleanMap(Config.content, ['vars']).isEmpty())

        Config.SetValue('vars.test_var', 'variable value')
        assertEquals(Config.vars.test_var, 'variable value')

        Config.LoadConfigFile(configFile)
        println MapUtils.ToJson(Config.content)
        assertEquals(Config.content.var, 'variable value')
    }
}
