package getl.lang

class GetlTest extends getl.test.GetlTest {
    void testJdbc() {
        Getl.run {
            log {
                logFileName = "$tempPath.systemPath/getl.script.log"
            }

            def table1 = table {
                connection = tempdb
                tableName = 'table1'

                field = [
                        field { name = 'id'; type = integerFieldType; isKey = true },
                        field { name = 'name'; type = stringFieldType; length = 50; isNull = false },
                        field { name = 'dt'; type = datetimeFieldType; defaultValue = 'Now()'; isNull = false }
                ]

                create()
                logInfo 'table1 created'
            }

            rowsTo {
                dest = table1

                process = { append ->
                    (1..3).each { append id: it, name: "test $it", dt: now }
                }
                onDone = { logInfo "$countRow rows appended to table1" }
            }


            def file1 = tempFile {
                fileName = 'getl.lang.csv'
                fieldDelimiter = ','
                codePage = 'UTF-8'
                isGzFile = true
            }

            copyRows {
                inheritFields = true

                source = table1
                dest = file1

                onDone = {
                    assertEquals(3, countRow)
                    logInfo "copied $countRow rows from table1 to file1" }
            }

            rowsToMany {
                dest = [
                        table1: table(table1) { truncate() },
                        table2: table {
                            connection = table1.connection
                            tableName = 'table2'
                            field = table1.field
                            create()
                            logInfo 'table2 created'
                        }
                ]

                bulkLoad = true

                def readRows = 0
                process = { save ->
                    rowProcess {
                        source = file1
                        process = { row ->
                            save 'table1', row
                            save 'table2', row
                        }

                        onDone = { readRows = countRow }
                    }
                }

                onDone = {
                    assertEquals(3, readRows);
                    logInfo "load $readRows rows from file1 to table1 and table2" }
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
                onInit = { count = 0 }
                process = { count++; assertEquals(it.t1_id, it.t2_id) }
                onDone = {
                    assertEquals(3, count)
                    assertEquals(countRow, count)
                    logInfo "read $countRow rows from table1 and table2"
                }
            }

            rowProcess {
                source = table(table1) { where = 'id < 3'; order = ['id ASC'] }
                process = { assertTrue(it.id < 3) }
                onDone = {
                    assertEquals(2, countRow)
                    logInfo("readed $countRow from table1 with filter")
                }
            }
        }
    }
}