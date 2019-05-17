package getl.lang

import getl.config.ConfigFiles
import getl.config.ConfigSlurper
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
    
    csv {
        fileName = 'getl.lang.csv'
    }
}
''', 'UTF-8')

        Getl.run {
            config {
                path = tempPath
                load('getl.conf')
                done { logInfo 'Load config' }
            }
            assertEquals('getl.lang.csv', configContent.datasets?.csv?.fileName)
            assertEquals('table1', configContent.datasets?.table1?.tableName)

            log {
                done { logInfo 'Setting logger' }
            }

            def table1 = h2table { H2Table table ->
                config = 'table1'
                connection = tempdb { sqlHistoryFile = "$tempPath/getl.lang.h2.sql" }

                field = [
                        field { name = 'id'; type = integerFieldType; isKey = true },
                        field { name = 'name'; type = stringFieldType; length = 50; isNull = false },
                        field { name = 'dt'; type = datetimeFieldType; defaultValue = 'Now()'; isNull = false }
                ]

                createTable {
                    type = isTemporary
                    transactional = true
                    ifNotExists = false
                    hashPrimaryKey = true
                    indexes.index_1 = index {
                        ifNotExists = true
                        columns = ['dt']
                        unique = false
                        logInfo "Create index_1 in $table"
                    }
                    done {
                        assertEquals('table1', table.tableName)
                        logInfo "$table created"
                    }

                    logInfo "Create $table"
                }

                rowsTo {
                    dest = table

                    process { append ->
                        (1..3).each { append id: it, name: "test $it", dt: now }
                        logInfo "Append rows to $table"
                    }
                    done {
                        assertEquals(3, table.countRows())
                        logInfo "$countRow rows appended to $table"
                    }

                    logInfo "Write to $table"
                }

                copyRows {
                    inheritFields = true

                    source = table
                    dest = csvTemp {
                        config = 'csv'
                        fieldDelimiter = ','
                        codePage = 'UTF-8'
                        isGzFile = true
                    }

                    process { t, f ->
                        f.name = StringUtils.ToCamelCase(t.name)
                        f.dt = now
                    }

                    done {
                        assertEquals(3, countRow)
                        logInfo "copied $countRow rows from $source to $dest"
                    }

                    logInfo "Copy rows from $table to $csvTemp"
                }

                logInfo "$table processed."
            }

            def file1 = csvTemp {
                config = 'csv'
                fieldDelimiter = ','
                codePage = 'UTF-8'
                isGzFile = true
                logInfo "Csv temporary file"
            }
            assertEquals('getl.lang.csv', file1.fileName)

            rowsToMany {
                dest = [
                        table1: table(table1) { truncate() },
                        table2: h2table { table ->
                            connection = table1.connection
                            tableName = 'table2'
                            field = table1.field
                            createTable {
                                type = isTemporary
                                transactional = true
                                ifNotExists = false
                                hashPrimaryKey = true
                            }
                            logInfo "$table created"
                        }
                ]

                bulkLoad = true

                def readRows = 0
                process { save ->
                    rowProcess {
                        source = file1
                        process { row ->
                            row.dt = now
                            save 'table1', row
                            save 'table2', row
                        }

                        done { readRows = countRow; logInfo "Read $file1 processed." }

                        logInfo "Read rows from $file1"
                    }
                }

                done {
                    assertEquals(3, readRows);
                    logInfo "load $readRows rows from $file1 to ${dest.table1} and ${dest.table2}"
                }

                logInfo "Copy to many destinations"
            }

            rowProcess {
                source = query {
                    connection = table1.connection
                    query = '''
SELECT
    t1.id as t1_id, t1.name as t1_name, t1.dt as t1_dt,
    t2.id as t2_id, t2.name as t2_name, t2.dt as t2_dt
FROM table1 t1 
    INNER JOIN table2 t2 ON t1.id = t2.id
ORDER BY t1.id'''
                }

                def count
                init { count = 0 }
                process { count++; assertEquals(it.t1_id, it.t2_id); assertTrue(it.t1_dt < now ) }
                done {
                    assertEquals(3, count)
                    assertEquals(countRow, count)
                    logInfo "read $countRow rows from table1 and table2"
                }

                logInfo "Read all rows from $table1"
            }

            rowProcess {
                source = table(table1) { where = 'id < 3'; order = ['id ASC'] }
                process { assertTrue(it.id < 3); assertTrue(it.t1_dt < now ) }
                done {
                    assertEquals(2, countRow)
                    logInfo("readed $countRow from $source with filter")
                }

                logInfo "Read rows with filter from $table1"
            }
        }
        Config.configClassManager = new ConfigFiles()
    }
}