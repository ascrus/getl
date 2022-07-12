package getl.impala

import getl.data.Field
import getl.jdbc.JDBCConnection
import getl.jdbc.JDBCDriverProto
import getl.jdbc.TableDataset
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
    protected String getTablePrefix() { '_impala' }

    @Override
    String getDescriptionName() { 'description' }

    @Test
    void testImpalaDsl() {
        if (con == null)
            return

        Getl.Dsl(this) {
            useImpalaConnection this.con as ImpalaConnection

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

    @Override
    protected void prepareTable() {
        (table as ImpalaTable).createOpts {
            storedAs = 'PARQUET'
        }
    }

    @Override
    protected getLineFeedChar() { '' }

    @Override
    protected prepareBulkTable(TableDataset table) {
        (table as ImpalaTable).tap {
            createOpts {
                storedAs = 'PARQUET'
            }
        }
    }

    @Override
    protected TableDataset createPerformanceTable(JDBCConnection con, String name, List<Field> fields) {
        def t = new ImpalaTable(connection: con, schemaName: con.connectDatabase, tableName: name, field: fields)
        t.drop(ifExists: true)
        t.create(storedAs: 'PARQUET')
        return t
    }

    @Test
    void bulkLoadFiles() {
        Getl.Dsl(this) {
            def ht = impalaTable {
                connection = this.con
                tableName = 'getl_test_bulkload_impala'
                field('id') { type = integerFieldType; isKey = true }
                field('name') { length = 50; isNull = false }
                field('dt') { type = datetimeFieldType }
                drop(ifExists: true)
                create()
            }

            def csv = csvTempWithDataset(ht) {
                fileName = 'impala.bulkload'
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
                    files = "impala.bulkload.*.csv"
                    loadAsPackage = true
                }
            }

            assertEquals(3, ht.updateRows)
            assertEquals(3, ht.countRow())
        }
    }
}