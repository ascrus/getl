package getl.h2

import getl.data.Field
import getl.jdbc.JDBCConnection
import getl.jdbc.JDBCDriver
import getl.jdbc.JDBCDriverProto
import getl.jdbc.QueryDataset
import getl.lang.Getl
import getl.proc.Flow
import getl.tfs.TDS
import getl.utils.Config
import getl.utils.FileUtils
import groovy.transform.InheritConstructors
import org.h2.value.ValueTimestampTimeZone
import org.junit.Test

/**
 * Created by ascru on 21.11.2016.
 */
@InheritConstructors
class H2DriverTest extends JDBCDriverProto {
	static final def configName = 'tests/h2/h2.conf'

    @Override
    protected JDBCConnection newCon() {
		if (FileUtils.ExistsFile(configName)) Config.LoadConfig(fileName: configName)
        needCatalog = 'GETL'
		return new TDS()
	}

    @Test
    void testVersion() {
        def q = new QueryDataset(connection: con, query: 'SELECT H2Version() AS version')
        def r = q.rows()
        assertEquals(1, r.size())
        assertEquals('1.4.200', r[0].version)
    }

    @Test
    void testSessionProperties() {
        def c = new H2Connection(inMemory: true, sessionProperty: ['exclusive': 1])
        def q = new QueryDataset(connection: c, query: 'SELECT VALUE FROM INFORMATION_SCHEMA.SETTINGS WHERE NAME = \'EXCLUSIVE\'')

        c.connected = true
        assertEquals('TRUE', q.rows().get(0).value)

        c.exclusive = 0
        assertEquals('FALSE', q.rows().get(0).value)

		c.connected = false
    }

    @Test
    void testCaseName () {
        con.executeCommand(command: 'CREATE SCHEMA test; CREATE TABLE test."$Test_Chars" (Id int NOT NULL, name varchar(50));')
        assertFalse((con.driver as JDBCDriver).retrieveObjects(schemaName: 'test', tableName: '$Test_Chars', null).isEmpty())
    }

    void testTZ() {
        def ds = TDS.dataset()
        ds.tableName = 'test'
        ds.field << new Field(name: 'id', type: Field.integerFieldType, isKey: true)
        ds.field << new Field(name: 'dt', type: Field.datetimeFieldType)
        ds.field << new Field(name: 'dtz', type: Field.timestamp_with_timezoneFieldType)
        ds.create()
        new Flow().writeTo(dest: ds) { updater ->
            Map r = [:]
            r.id = 1
            r.dt = new Date()
            r.dtz = new Date()
            updater(r)
        }

        ds.field.clear()

        (ds.connection as JDBCConnection).sqlConnection.eachRow('select * from test') { r ->
            def t = r.dtz as org.h2.api.TimestampWithTimeZone
            println t
            def x = ValueTimestampTimeZone.fromDateValueAndNanos(t.YMD, t.nanosSinceMidnight, t.timeZoneOffsetSeconds).getTimestamp(TimeZone.default)
            println x
        }

        ds.eachRow { r ->
            println r
        }
    }

    @Test
    void testViewFields() {
        Getl.Dsl {
            con.with {
                def tab = h2Table {
                    tableName = 'test_view'
                    field('id') { type = integerFieldType; isKey = true }
                    field('name') { length = 50; isNull = false }
                    field('value') { type = numericFieldType; length = 12; precision = 2 }
                    create(ifNotExists: true)
                }
                executeCommand 'CREATE OR REPLACE VIEW v_test_view AS SELECT * FROM test_view'
                def view = view {
                    tableName = 'v_test_view'
                    retrieveFields()
                    assertEquals(3, field.size())
                    field.each {assertTrue(it.isNull) }
                }
                view.drop()
                tab.drop()
            }
        }
    }
}