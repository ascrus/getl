package getl.config

import getl.csv.CSVConnection
import getl.h2.H2Connection
import getl.tfs.TFS
import getl.utils.Config
import getl.utils.MapUtils
import getl.utils.MapUtilsTest
import groovy.json.JsonBuilder

/**
 * @author Alexsey Konstantinov
 */
class ConfigSlurperTest extends getl.test.GetlTest {
    def test = false

    def h2 = new H2Connection(config: 'h2')
    def csv = new CSVConnection(config: 'csv')

    void testLoadConfig() {
        Config.configClassManager = new ConfigSlurper()

        def configPath = new TFS(path: (test)?'c:/tmp':null)
        def configFile = new File("${configPath.path}/test_config.conf")
        if (!test) configFile.deleteOnExit()

        def conf = '''
            map {
                list=['a', 1, "${vars.test_var}", null]
            }
            var1="${vars.test_var}"
            var2=Date.parse('yyyy-MM-dd HH:mm:ss', '2019-02-01 01:02:03')
            var3=[
                {
                    a=1
                    b=2
                    c=3
                    d="${vars.test_var}"
                },
                {
                    a=4
                    b=5
                    c=6
                    d="${vars.test_var}"
                }
            ]

            connections {
                h2 {
                    connectURL='jdbc:h2:tcp://localhost/test'
                    login='sa'
                    password='test'
                    connectProperty {
                        db_close_delay=-1
                    }
                }
                csv {
                    path='.'
                    rowDelimiter='\\r\\n'
                }
            }
        '''
        configFile.setText(conf, 'utf-8')
        assertTrue(configFile.exists())

        Config.SetValue('vars.test_var', 'variable value')
        assertEquals(Config.vars.test_var, 'variable value')

        Config.LoadConfig(fileName: configFile)

        assertEquals('jdbc:h2:tcp://localhost/test', h2.connectURL)
        assertEquals('sa', h2.login)
        assertEquals('test', h2.password)
        assertEquals(-1, h2.connectProperty.db_close_delay)

        assertEquals('.', csv.path)
        assertEquals('\r\n', csv.rowDelimiter)

        assertEquals('variable value', Config.content.var1)
        assertEquals(Date.parse('yyyy-MM-dd HH:mm:ss', '2019-02-01 01:02:03'), Config.content.var2)

        assertEquals([a:1,b:2,c:3,d:'variable value'], Config.content.var3[0])
        assertEquals([a:4,b:5,c:6,d:'variable value'], Config.content.var3[1])

        assertEquals(['a', 1, 'variable value', null], Config.content.map.list)

        (Config.configClassManager as ConfigSlurper).path = configPath
        Config.content.var1 = '${vars.test_var}'
        Config.content.var3[0].d = '${vars.test_var}'
        Config.content.var3[1].d = '${vars.config_var}'
        Config.SaveConfig(fileName: 'test_config.groovy')
        def groovyFile = new File("${configPath.path}/test_config.groovy")
        if (!test) groovyFile.deleteOnExit()

        Config.ClearConfig()
        Config.SetValue('vars.config_var', 'variable value')
        Config.LoadConfig(fileName: configPath.path + '/' + 'test_config.groovy')
        assertEquals(Config.content.var1, 'variable value')
        assertEquals(Date.parse('yyyy-MM-dd HH:mm:ss', '2019-02-01 01:02:03'), Config.content.var2)
        assertEquals([a:1,b:2,c:3,d:'variable value'], Config.content.var3[0])
        assertEquals([a:4,b:5,c:6,d:'variable value'], Config.content.var3[1])
    }
}