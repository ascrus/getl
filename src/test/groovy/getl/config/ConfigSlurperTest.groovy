package getl.config

import getl.csv.CSVConnection
import getl.h2.H2Connection
import getl.lang.Getl
import getl.tfs.TFS
import getl.utils.Config
import getl.utils.FileUtils
import getl.utils.MapUtils
import getl.utils.MapUtilsTest
import groovy.json.JsonBuilder
import groovy.transform.InheritConstructors
import org.apache.kerby.config.Conf
import org.junit.After
import org.junit.Before
import org.junit.Test

/**
 * @author Alexsey Konstantinov
 */
@InheritConstructors
class ConfigSlurperTest extends getl.test.GetlTest {
    def configPath = new TFS()
    def configFile = new File("${configPath.currentPath()}/test_config.getl")

    def h2 = new H2Connection(config: 'h2')
    def csv = new CSVConnection(config: 'csv')

    @Before
    void setUp() {
        Config.configClassManager = new ConfigSlurper()
        (Config.configClassManager as ConfigSlurper).path = configPath.currentPath()

        if (configFile.exists()) return

        def conf = '''
            configvars {
              local_var = 'local variable value'
            }

            map {
                list=['a', 1, "${vars.config_var}", "${configvars.local_var}", null]
            }
            var1="${vars.config_var}".toString()
            var2='2019-02-01 01:02:03'
            var3=[
                {
                    a=1
                    b=2
                    c=3
                    d="${vars.config_var}"
                },
                {
                    a=4
                    b=5
                    c=6
                    d="${vars.config_var}"
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
        configFile.deleteOnExit()
    }

    @Test
    void testLoadConfig() {
        assertTrue(configFile.exists())

        Config.SetValue('vars.config_var', 'variable value')
        assertEquals(Config.vars.config_var, 'variable value')
        Config.LoadConfig(fileName: configFile.path)
        assertEquals(Config.vars.config_var, 'variable value')
        assertEquals(Config.vars.local_var, 'local variable value')

        assertEquals('jdbc:h2:tcp://localhost/test', h2.connectURL)
        assertEquals('sa', h2.login)
        assertEquals('test', h2.password)
        assertEquals(-1, h2.connectProperty.db_close_delay)

        assertEquals('.', csv.currentPath())
        assertEquals('\r\n', csv.rowDelimiter)

        assertEquals('variable value', Config.content.var1)
        assertEquals('2019-02-01 01:02:03', Config.content.var2)

        assertTrue([a:1,b:2,c:3,d:'variable value'].equals(Config.content.var3[0]))
        assertTrue([a:4,b:5,c:6,d:'variable value'].equals(Config.content.var3[1]))

        assertTrue(['a', 1, 'variable value', 'local variable value', null].equals(Config.content.map.list))

        Config.content.var1 = '${configvars.local_var}'
        Config.content.var3[0].d = '${configvars.local_var}'
        Config.content.var3[0].put(1, 111)
        Config.content.var3[1].d = '${vars.config_var}'
        Config.content.var3[1].put('_1', 222)
        Config.vars.remove('config_var')
        (Config.content.map as Map)."a1" = 1
        (Config.content.map as Map)."_a1" = 2
        (Config.content.map as Map)."1" = 3
        (Config.content.map as Map)."_1" = 4
        (Config.content.map as Map)."1_1" = 5
        (Config.content.map as Map)."\$1" = 6
        (Config.content.map as Map)."a#" = 7
        (Config.content.map as Map).'"1"' = 8
        Config.SaveConfig(fileName: 'test_config.groovy')
        Getl.pause(250)
        def groovyFile = new File("${configPath.currentPath()}/test_config.groovy")
        groovyFile.deleteOnExit()
        def fileDate = groovyFile.lastModified()
        Config.SaveConfig(fileName: 'test_config.groovy', smartWrite: true)
        assertEquals(fileDate, groovyFile.lastModified())
//        println groovyFile.text

        Config.ClearConfig()
        Config.SetValue('vars.config_var', 'variable value')
        Config.LoadConfig(fileName: configPath.currentPath() + '/' + 'test_config.groovy')
//        println '----------------'
//        println MapUtils.ToJson(Config.content)
        assertEquals(Config.content.var1.toString(), 'local variable value')
        assertEquals('2019-02-01 01:02:03', Config.content.var2)
        assertTrue([a:1,b:2,c:3,d:'local variable value', '1': 111].equals(Config.content.var3[0]))
        assertTrue([a:4,b:5,c:6,d:'variable value', '_1': 222].equals(Config.content.var3[1]))
        assertEquals(1, (Config.content.map as Map)."a1")
        assertEquals(2, (Config.content.map as Map)."_a1")
        assertEquals(3, (Config.content.map as Map)."1")
        assertEquals(4, (Config.content.map as Map)."_1")
        assertEquals(5, (Config.content.map as Map)."1_1")
        assertEquals(6, (Config.content.map as Map)."\$1")
        assertEquals(7, (Config.content.map as Map)."a#")
        assertEquals(8, (Config.content.map as Map).'"1"')
    }

    @Test
    void testDatasetSchema() {
        Getl.Dsl(this) {
            configuration { load configFile.path }

            csv {
                field('id') { type = integerFieldType; isKey = true }
                field('name') { length = 50; isNull = false }
                field('dt') { type = dateFieldType; isPartition = true }

                def f = new File(TFS.systemPath + '/demo.schema')
                f.deleteOnExit()
                saveDatasetMetadataToSlurper(f)

                field.clear()
                loadDatasetMetadataFromSlurper(f)
                assertEquals(3, field.size())
                assertTrue(field('id').isKey)
                assertFalse(field('name').isNull)
                assertTrue(field('dt').isPartition
                )
            }
        }
    }

    @Test
    void testLoadDatasets() {
        Getl.Dsl {
            configuration {
                configuration { load configFile.path }
                load 'resource:/config/source.dwh.conf'
            }
        }
    }
}