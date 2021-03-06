package getl.impala

import getl.jdbc.JDBCConnection
import getl.jdbc.JDBCDriverProto
import getl.lang.Getl
import getl.utils.Config
import getl.utils.DateUtils
import getl.utils.FileUtils
import groovy.transform.InheritConstructors
import org.junit.Test

@InheritConstructors
class ImpalaDriverTest extends JDBCDriverProto {
    static final def configName = 'tests/impala/impala.conf'

    @Override
    protected JDBCConnection newCon() {
        if (!FileUtils.ExistsFile(configName)) return null
        Config.LoadConfig(fileName: configName)
        needCatalog = 'impala'
        return new ImpalaConnection(config: 'impala')
    }

    @Override
    String getUseTableName() { 'getl_test_impala' }

    @Override
    protected void createTable() {
        ImpalaTable t = table as ImpalaTable
        t.schemaName = con.connectDatabase
        t.drop(ifExists: true)
        t.field = fields
        t.create(storedAs: 'PARQUET', sortBy: ['id1'], tblproperties: [transactional: false])
    }

    @Override
    String getDescriptionName() { "description" }

    @Test
    void testImpalaDsl() {
        if (con == null)
            return

        Getl.Dsl(this) {
            useImpalaConnection this.con

            impalaTable {
                schemaName = currentImpalaConnection.schemaName
                tableName = 'getl_test_impala_dsl'
                field('id') { type = bigintFieldType }
                field('name')
                field('dt') { type = datetimeFieldType}
                field('flag') { type = booleanFieldType }
                field('double') { type = doubleFieldType }
                field('value') { type = numericFieldType; length = 12; precision = 2 }
                field('part_day') { type = integerFieldType; isPartition = true }

                dropOpts {
                    ifExists = true
                }

                createOpts {
                    storedAs = 'PARQUET'
                    sortBy = ['id']
                    tblproperties.transactional = false
                }

                writeOpts {
                    compression = gzipCompressionCodec
                }

                readOpts {
                    order = ['id']
                }

                drop()
                create()
                assertEquals(0, countRow())

                def countWrite = etl.rowsTo {
                    writeRow() { add ->
                        (1..3).each { num ->
                            add id: num, name: "name $num", dt: DateUtils.Now(), flag: true,
                                    double: Float.valueOf('123.45'), number: new BigDecimal('123.45'),
                                    part_day: DateUtils.FormatDate('yyyyMMdd', DateUtils.Now()).toInteger()
                        }
                    }
                }.countRow
                assertEquals(3, countWrite)
                assertEquals(3, countRow())

                def i = 0
                eachRow { row ->
                    i++
                    assertEquals(i, row.id)
                }
            }
        }
    }
}