package getl.hive

import getl.jdbc.JDBCConnection
import getl.jdbc.TableDataset
import getl.proc.Flow
import getl.utils.Config
import getl.utils.FileUtils

/**
 * Created by ascru on 23.03.2017.
 */
class HiveDriverTest extends GroovyTestCase {
    static final def configName = 'tests/hive/hive.conf'

    protected JDBCConnection newCon() {
        if (!FileUtils.ExistsFile(configName)) return null
        Config.LoadConfig(configName)
        return new HiveConnection(config: 'hive')
    }

    public void testEachRow() {
        def t = new TableDataset(connection: newCon(), tableName: 'test3')
        t.eachRow { println it }
    }

    public void testInsert() {
        def t = new TableDataset(connection: newCon(), tableName: 'test3')
        t.truncate(truncate: true)
        new Flow().writeTo(dest: t) { Closure updater ->
            (1..10).each { num ->
                Map r = [id: num, name: "value $num", value: new Double(num) + 0.123]
                updater(r)
            }
        }
    }
}
