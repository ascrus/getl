package getl.clickhouse

import getl.jdbc.JDBCConnection
import getl.jdbc.JDBCDriverProto
import getl.lang.Getl
import getl.sap.HanaConnection
import getl.utils.Config
import getl.utils.DateUtils
import getl.utils.FileUtils
import groovy.transform.InheritConstructors
import org.junit.Test

@InheritConstructors
class ClickHouseDriverTest extends JDBCDriverProto {
    static final def configName = 'tests/clickhouse/clickhouse.conf'
    @Override
    protected JDBCConnection newCon() {
        if (!FileUtils.ExistsFile(configName)) return null
        Config.LoadConfig(fileName: configName)
        return new ClickHouseConnection(config: 'clickhouse')
    }

    @Override
    protected String getCurrentTimestampFuncName() { 'NOW64()' }

    @Test
    void testCreateTable() {
        Config.LoadConfig(fileName: configName)
        Getl.Dsl {
            def tab1 = clickhouseTable {
                useConnection clickhouseConnection { config = 'clickhouse'}
                schemaName = 'dwh'
                tableName = 'getl_test_ch_table'
                field('master_id') { type = integerFieldType; isKey = true; ordKey = 1 }
                field('dt') { type = dateFieldType; isKey = true; ordKey = 2 }
                field('value') { type = numericFieldType; length = 12; precision = 2 }
                createOpts {
                    engine = 'MergeTree'
                    partitionBy = 'toYYYYMM(dt)'
                    orderBy = ['master_id', 'dt']
                }
                if (exists)
                    drop()
                create()
                etl.rowsTo {
                    writeRow { adder ->
                        (1..100000).each { num ->
                            adder master_id: num, dt: DateUtils.CurrentDate(), value: num + 0.12
                        }
                    }
                }
                assertEquals(100000, countRow())
            }

            def tabs = tab1.currentClickHouseConnection.retrieveDatasets(schemaName: 'dwh', tableName: 'getl_test_ch_table')
            assertEquals(1, tabs.size())

            def tab2 = tabs[0] as ClickHouseTable
            tab2.tap {
                resetFieldsTypeName()
                assertEquals(tab1.field, field)
                createOpts {
                    assertEquals(tab1.createOpts.engine, engine)
                    assertEquals(tab1.createOpts.partitionBy, partitionBy)
                    assertEquals(tab1.createOpts.orderBy, orderBy)
                }
            }
        }
    }
}