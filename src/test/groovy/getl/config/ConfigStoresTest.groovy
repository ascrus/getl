package getl.config

import getl.h2.H2Connection
import getl.test.GetlTest
import getl.tfs.TFS
import getl.utils.Config
import getl.utils.MapUtils
import groovy.json.JsonBuilder
import org.junit.Test

class ConfigStoresTest extends GetlTest {
    def h2 = new H2Connection(config: 'h2')

    @Test
    void testSaveLoadConfig() {
        def manager = new ConfigStores()
        Config.configClassManager = manager

        def configPath = new TFS()
        def configFile = new File("${configPath.currentPath()}/test_config.store")
        configFile.deleteOnExit()
        def configSection = 'test_config'
        def configKey = 'test key'

        def builder = new JsonBuilder()
        def conf = builder.root {
            map {
                list 'a', 1, null
            }
            var '${test_var}'

            connections {
                h2 {
                    connectURL 'jdbc:h2:tcp://localhost/test'
                    login 'sa'
                    password 'test'
                    connectProperty {
                        db_close_delay "-1"
                    }
                }
                csv {
                    path '.'
                    rowDelimiter '\r\n'
                }
            }
        }

        Config.content.putAll(conf.root)
        Config.SaveConfig(fileName: configFile, section: configSection, secretKey: configKey)
        assertTrue(configFile.exists())
//        println "$configFile: ${configFile.size()}"

        Config.ClearConfig()
        assertTrue(MapUtils.CleanMap(Config.content, ['vars']).isEmpty())

        Config.SetValue('vars.test_var', 'variable value')
        assertEquals('variable value', Config.vars.test_var)

        def valid = {
            assertEquals('variable value', Config.content.var)

            assertEquals('jdbc:h2:tcp://localhost/test', h2.connectURL)
            assertEquals('sa', h2.login)
            assertEquals('test', h2.password)
            assertEquals('-1', h2.connectProperty.db_close_delay)
        }

        Config.LoadConfig(fileName: configFile, section: configSection, secretKey: configKey)
        valid()

        manager.SaveSection([save2: 'ok'], configFile.path, configKey, configSection)
        Config.ClearConfig()
        Config.SetValue('vars.test_var', 'variable value')
        Config.LoadConfig(fileName: configFile, section: configSection, secretKey: configKey)
        valid()
        assertEquals('ok', Config.content.save2)
    }
}