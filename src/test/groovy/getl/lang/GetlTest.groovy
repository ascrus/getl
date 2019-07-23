package getl.lang

import getl.config.ConfigFiles
import getl.h2.*
import getl.tfs.*
import getl.utils.Config
import getl.utils.StringUtils

class GetlTest extends getl.test.GetlTest {
    void testEtl() {
        def tempPath = new TFS()
        def tempConfig = new File("$tempPath/getl.conf")
        tempConfig.deleteOnExit()
        tempConfig.setText('''
datasets {
    table1 {
        tableName = 'table1'
    }
    
    file1 {
        fileName = 'getl.lang.csv'
    }
}
''', 'UTF-8')

        Getl.Dsl {
            configuration {
                path = tempPath
                load('getl.conf')
                done { logInfo 'Load config' }
            }
            assertEquals('getl.lang.csv', configContent.datasets?.file1?.fileName)
            assertEquals('table1', configContent.datasets?.table1?.tableName)

            logging {
                logFileName = csvTempConnection().path + '/getl.dsl.logs'
            }

            useH2Connection embeddedConnection('h2', true) { sqlHistoryFile = "$tempPath/getl.lang.h2.sql" }

            h2Table('table1', true) { H2Table table ->
                config = 'table1'

                field = [
                        field('id') { type = integerFieldType; isKey = true },
                        field('name') { type = stringFieldType; length = 50; isNull = false },
                        field('dt') { type = datetimeFieldType; defaultValue = 'Now()'; isNull = false }
                ]

                createOpts {
                    type = isTemporary
                    transactional = true
                    ifNotExists = false
                    hashPrimaryKey = true
                    index('idx_1') {
                        ifNotExists = true
                        columns = ['dt']
                        unique = false
                    }
                }
                assertEquals('table1', table.tableName)
                create()
                assertTrue(exists)

                rowsTo(table) {
                    process { append ->
                        (1..3).each { append id: it, name: "test $it", dt: now }
                    }
                    done {
                        assertEquals(3, table.countRow())
                    }
                }

                csvTempWithDataset('file1', h2Table('table1')) { config = 'file1' }
                copyRows(h2Table('table1'), csvTemp('file1')) {
                    process { t, f ->
                        f.name = StringUtils.ToCamelCase(t.name)
                        f.dt = now
                    }

                    done {
                        assertEquals(3, countRow)
                    }
                }
                assertEquals('getl.lang.csv', csvTemp('file1').fileName)
            }

            rowsToMany([
                        table1: h2Table('table1') { truncate() },
                        table2: h2Table('table2', true) { table ->
                            tableName = 'table2'
                            field = h2Table('table1').field
                            createOpts {
                                type = isTemporary
                                transactional = true
                                ifNotExists = false
                                hashPrimaryKey = true
                            }
                            create()
                        }
                ]) {
                bulkLoad = true

                def readRow = 0
                process { save ->
                    rowProcess(csvTemp('file1')) {
                        process { row ->
                            row.dt = now
                            save 'table1', row
                            save 'table2', row
                        }

                        done { readRow = countRow }
                    }
                }

                done {
                    assertEquals(3, readRow);
                }
            }

            query('query1', true) {
                query = '''
SELECT
    t1.id as t1_id, t1.name as t1_name, t1.dt as t1_dt,
    t2.id as t2_id, t2.name as t2_name, t2.dt as t2_dt
FROM table1 t1 
    INNER JOIN table2 t2 ON t1.id = t2.id
ORDER BY t1.id'''
            }

            rowProcess(query('query1')) {
                def count
                init { count = 0 }
                process { count++; assertEquals(it.t1_id, it.t2_id); assertTrue(it.t1_dt < now ) }
                done {
                    assertEquals(3, count)
                    assertEquals(countRow, count)
                }
            }

            rowProcess(h2Table('table1') { readOpts { where = 'id < 3'; order = ['id ASC'] } }) {
                process { assertTrue(it.id < 3); assertTrue(it.t1_dt < now ) }
                done {
                    assertEquals(2, countRow)
                }
            }
        }
        Config.configClassManager = new ConfigFiles()
    }
}