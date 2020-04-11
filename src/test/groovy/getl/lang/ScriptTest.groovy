package getl.lang

import getl.jdbc.JDBCConnection
import getl.jdbc.TableDataset
import getl.test.GetlDslTest
import groovy.transform.InheritConstructors
import groovy.transform.stc.ClosureParams
import groovy.transform.stc.SimpleType
import org.junit.Test

@InheritConstructors
class ScriptTest extends GetlDslTest {
    def field = 1

    JDBCConnection connection(String name,
                              @DelegatesTo(JDBCConnection)
                              @ClosureParams(value = SimpleType, options = ['getl.jdbc.JDBCConnection']) Closure cl) {
        def parent = new JDBCConnection()
        parent.with(cl)
        return parent
    }

    TableDataset table(String name,
                @DelegatesTo(TableDataset)
                @ClosureParams(value = SimpleType, options = ['getl.jdbc.TableDataset']) Closure cl) {
        def parent = new TableDataset()
        parent.with(cl)
        return parent
    }

    @Test
    void testLocalVar() {
        table('table1') {
            tableName = "localtable"
            connection = connection('connection1') {
                connectHost = 'localhost'
                connectDatabase = 'test'
                println "connection1: $it"
                println "table1: $it"
            }
            println "connection1: $connection"
            println "table1: $it"
        }
    }
}