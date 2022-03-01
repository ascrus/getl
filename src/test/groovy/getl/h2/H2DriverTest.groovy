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
import org.junit.Test

import java.sql.Timestamp

/**
 * Created by ascru on 21.11.2016.
 */
@InheritConstructors
class H2DriverTest extends JDBCDriverProto {
	static final def configName = 'tests/h2/h2.conf'

    @Override
    protected JDBCConnection newCon() {
		if (FileUtils.ExistsFile(configName))
            Config.LoadConfig(fileName: configName)
		def res = new TDS(connectDatabase: TDS.storageDatabaseName)
        needCatalog = res.connectDatabase.toUpperCase()
        return res
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
        def c = new H2Connection(inMemory: true, sessionProperty: [exclusive: 1])

        c.connected = true
        assertEquals('TRUE', c.readSettingValue('EXCLUSIVE'))

        c.exclusive = 0
        assertEquals('FALSE', c.readSettingValue('EXCLUSIVE'))

		c.connected = false
    }

    @Test
    void testCaseName () {
        con.executeCommand(command: 'CREATE SCHEMA test; CREATE TABLE test."$Test_Chars" (Id int NOT NULL, name varchar(50));')
        assertFalse((con.driver as JDBCDriver).retrieveObjects(schemaName: 'test', tableName: '$Test_Chars', null).isEmpty())
    }

    @Test
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
        assertTrue(ds.rows()[0].dtz instanceof Timestamp)
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

    @Test
    void testBulkLoadMap() {
        Getl.Dsl {
            def table1 = embeddedTable {
                field('id') { type = integerFieldType; isKey = true }
                field('name') { length = 50; isNull = false }
                field('value') { type = numericFieldType; length = 12; precision = 2 }
                create()
            }

            def file1 = csvTempWithDataset(table1) {
                field('description') { length = 50 }
                etl.rowsTo {
                    writeRow { add ->
                        (1..10).each { add id: it, name: "Name $it", value: it * 1.23, description: "Description $it" }
                    }
                }
            }

            table1.bulkLoadCsv(file1)
            assertEquals(10, table1.countRow())
            def i = 0
            table1.eachRow(order: ['id']) { row ->
                i++
                assertEquals(i, row.id)
                assertEquals("Name $i".toString(), row.name)
                assertEquals(i * 1.23, row.value)
            }

            table1.truncate()
            table1.bulkLoadCsv(file1) {
                map.name = 'description'
            }
            i = 0
            table1.eachRow(order: ['id']) { row ->
                i++
                assertEquals(i, row.id)
                assertEquals("Description $i".toString(), row.name)
                assertEquals(i * 1.23, row.value)
            }

            table1.truncate()
            shouldFail {
                table1.bulkLoadCsv(file1) {
                    allowExpressions = false
                    map.name = 'Upper(name)'
                }
            }

            table1.truncate()
            table1.bulkLoadCsv(file1) {
                map.name = 'Upper(name)'
                map.value = '#'
            }
            i = 0
            table1.eachRow(order: ['id']) { row ->
                i++
                assertEquals(i, row.id)
                assertEquals("NAME $i".toString(), row.name)
                assertNull(row.value)
            }

            table1.truncate()
            table1.bulkLoadCsv(file1) {
                map.name = 'Upper("DESCRIPTION")'
                map.value = 'Cast("VALUE" as Numeric(12, 2)) * 10'
            }
            i = 0
            table1.eachRow(order: ['id']) { row ->
                i++
                assertEquals(i, row.id)
                assertEquals("DESCRIPTION $i".toString(), row.name)
                assertEquals((i * 1.23) * 10, row.value)
            }
        }
    }
}