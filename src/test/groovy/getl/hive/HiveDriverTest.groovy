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
        needCatalog = 'hive'
        return new HiveConnection(config: 'hive')
    }

    @Override
    protected boolean getUseDate() { false }

    @Override
    String getUseTableName() { 'getl_test_hive' }

    @Override
    protected String localTablePrefix() { '' }

    @Override
    protected TableDataset createPerformanceTable(JDBCConnection con, String name, List<Field> fields) {
        HiveTable t = new HiveTable(connection: con, schemaName: con.connectDatabase, tableName: name, field: fields)
        t.drop(ifExists: true)
        t.create(storedAs: 'ORC', clustered: [by: ['id'], intoBuckets: 2], tblproperties: [transactional: false])
        return t
    }

    @Test
    void bulkLoadFiles() {
        Getl.Dsl(this) {
            def ht = hiveTable {
                connection = this.con
                tableName = 'testBulkLoad'
                type = localTemporaryTableType
                field('id') { type = integerFieldType; isKey = true }
                field('name') { length = 50; isNull = false }
                field('dt') { type = datetimeFieldType }
                create()
            }

            def csv = csvTempWithDataset(ht) {
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

            ht.tap {
                bulkLoadCsv(csv) {
                    files = "hive.bulkload.*.csv"
                    loadAsPackage = true
                }
            }

            assertEquals(3, ht.updateRows)
            assertEquals(3, ht.countRow())
        }
    }

    @Override
    protected void prepareTable() {
        (table as HiveTable).createOpts {
            storedAs = 'PARQUET'
        }
    }

    @Override
    protected getLineFeedChar() { '' }

    @Override
    protected prepareBulkTable(TableDataset table) {
        (table as HiveTable).tap {
            createOpts {
                storedAs = 'PARQUET'
            }
        }
    }
}