package getl.hive

import getl.data.*
import getl.jdbc.*
import getl.utils.*
import getl.lang.Getl
import groovy.transform.InheritConstructors
import org.junit.Test

@InheritConstructors
class HiveDriverTest extends JDBCDriverProto {
    static final def configName = 'tests/hive/hive.conf'

    @Override
    protected JDBCConnection newCon() {
        if (!FileUtils.ExistsFile(configName)) return null
        Config.LoadConfig(fileName: configName)
        return new HiveConnection(config: 'hive')
    }

    @Override
    protected boolean getUseDate() { false }

    @Override
    String getUseTableName() { 'getl_test_hive' }

    @Override
    protected void createTable() {
        HiveTable t = table as HiveTable
        t.schemaName = con.connectDatabase
        t.drop(ifExists: true)
        t.field = fields
        t.create(storedAs: 'ORC', clustered: [by: ['id1'], intoBuckets: 2], tblproperties: [transactional: false])
    }

    @Override
    protected TableDataset createPerfomanceTable(JDBCConnection con, String name, List<Field> fields) {
        HiveTable t = new HiveTable(connection: con, schemaName: con.connectDatabase, tableName: name, field: fields)
        t.drop(ifExists: true)
        t.create(storedAs: 'ORC', clustered: [by: ['id'], intoBuckets: 2], tblproperties: [transactional: false])
        return t
    }

    @Test
    void bulkLoadFiles() {
        Getl.Dsl(this) {
            def hivetable = hiveTable {
                connection = this.con
                tableName = 'testBulkLoad'
                type = localTemporaryTableType
                field('id') { type = integerFieldType; isKey = true }
                field('name') { length = 50; isNull = false }
                field('dt') { type = datetimeFieldType }
                create()
            }

            def csv = csvTempWithDataset(hivetable) {
                fileName = 'hive.bulkload'
                extension = 'csv'

                writeOpts {
                    splitFile { true }
                }

                etl.rowsTo {
                    writeRow { add ->
                        add id: 1, name: 'one', dt: DateUtils.Now()
                        add id: 2, name: 'one', dt: DateUtils.Now()
                        add id: 3, name: 'one', dt: DateUtils.Now()
                    }
                }
                assertEquals(3, writeRows)
                assertEquals(4, countWritePortions)
            }

            hivetable.with {
                bulkLoadCsv(csv) {
                    files = "hive.bulkload.*.csv"
                    loadAsPackage = true
                }
            }

            assertEquals(3, hivetable.updateRows)
            assertEquals(3, hivetable.countRow())
        }
    }
}