package getl.jdbc

import getl.lang.Getl
import getl.test.GetlTest
import getl.tfs.TDS
import getl.utils.DateUtils
import getl.utils.FileUtils
import getl.utils.GenerationUtils
import getl.utils.SQLParser
import getl.utils.SynchronizeObject
import groovy.transform.InheritConstructors
import org.junit.Ignore
import org.junit.Test

@InheritConstructors
class JDBCTest extends GetlTest {
    @Test
    void testFields() {
        Getl.Dsl(this) {
            embeddedTable {
                tableName = 'test_fields'
                field('id1') { type = integerFieldType; isKey = true; ordKey = 1 }
                field('id2') { type = datetimeFieldType; isKey = true; ordKey = 2 }
                field('name') { length = 50; isNull = false }
                field('value') { type = numericFieldType; length = 12; precision = 2 }

                create()
                assertNull(field('id1').typeName)
                assertTrue(field('id1').isKey)
                assertEquals(1, field('id1').ordKey)
                assertTrue(field('id2').isKey)
                assertEquals(2, field('id2').ordKey)

                etl.rowsTo {
                    writeRow { added ->
                        added id1: 1, id2: DateUtils.ParseDateTime('2019-12-31 23:59:59.000'), name: 'test', value: 123.45
                    }
                }
                assertNull(fieldByName('id1').typeName)
                assertTrue(fieldByName('id1').isKey)
                assertEquals(1, field('id1').ordKey)
                assertTrue(fieldByName('id2').isKey)
                assertEquals(2, fieldByName('id2').ordKey)

                eachRow {
                    assertNotNull(it.id1)
                }
                assertEquals('INTEGER', fieldByName('id1').typeName)
                assertTrue(fieldByName('id1').isKey)
                assertEquals(1, fieldByName('id1').ordKey)
                assertTrue(fieldByName('id2').isKey)
                assertEquals(2, fieldByName('id2').ordKey)

                retrieveFields()
                assertEquals('INTEGER', fieldByName('id1').typeName)
                assertTrue(fieldByName('id1').isKey)
                assertEquals(1, fieldByName('id1').ordKey)
                assertTrue(fieldByName('id2').isKey)
                assertEquals(2, fieldByName('id2').ordKey)

                assertEquals([1], fieldValues('id1'))

                drop()
            }
        }
    }

    @Test
    void testHistoryPointSave() {
        Getl.Dsl(this) {
            def tab = embeddedTable('test_history_point', true) { tableName = 'test_history_point' }

            historypoint {
                useHistoryTable tab

                sourceName = 'source1'
                sourceType = identitySourceType
                saveMethod = mergeSave

                create(true)
                assertEquals(4, tab.field.size())

                assertNull(lastValue())
                def id1 = 100
                saveValue id1
                assertEquals(id1, lastValue())
                assertEquals(1, tab.countRow())
                def id2 = 200
                saveValue id2
                assertEquals(id2, lastValue())
                assertEquals(1, tab.countRow())

                sourceName = 'source2'
                sourceType = timestampSourceType
                saveMethod = insertSave
                def dt1 = DateUtils.ParseSQLTimestamp('yyyy-MM-dd HH:mm:ss.SSSSSS', '2023-01-31 23:59:59.999999')
                saveValue dt1
                assertEquals(dt1, lastValue())
                assertEquals(2, tab.countRow())

                def dt2 = DateUtils.ParseSQLTimestamp('yyyy-MM-dd HH:mm:ss.SSSSSS', '2023-02-01 01:01:01.123456')
                saveValue dt2
                assertEquals(dt2, lastValue())
                assertEquals(3, tab.countRow())

                sourceName = 'source1'
                clearValue()
                assertNull(lastValue())

                sourceName = 'source2'
                truncate()
                assertNull(lastValue())
            }

            tab.drop()
        }
    }

    @Test
    void testHistoryPointThreads() {
        Getl.Dsl {
            def con = embeddedConnection('group:con', true) {
                sqlHistoryFile = '{GETL_TEST}/logs/emb.{date}.sql'
            }
            def tab = embeddedTable('group:history_table', true) {
                useConnection con
            }

            def obj1 = historypoint('group:hp1', true) {
                historyTable = tab
                assertEquals('group:history_table', historyTableName)
                historyTable = null
                assertNull(historyTableName)

                historyTableName = 'group:history_table'
                assertEquals(historyTable, tab)

                sourceName = 'source1'
                sourceType = identitySourceType
                saveMethod = mergeSave

                def lv = lastValue()
                assertNull(lv)

                saveValue(1)
                assertEquals(1, lastValue())
            }

            def dt = DateUtils.ParseSQLTimestamp('2020-12-31 00:00:00.000')
            def obj2 = cloneHistorypoint('group:hp2', obj1) {
                sourceName = 'source2'
                sourceType = timestampSourceType
                saveMethod = insertSave

                def lv = lastValue()
                assertNull(lv)

                saveValue(dt)
                assertEquals(dt, lastValue())
            }

            def s1 = new SynchronizeObject()
            def s2 = new SynchronizeObject()
            thread {
                abortOnError = true

                runMany(50) {
                    historypoint('group:hp1') {
                        def lv = lastValue() + GenerationUtils.GenerateInt(1, 50)
                        if (lv > s1.count)
                            s1.count = lv
                        saveValue lv
                    }
                    historypoint('group:hp2') {
                        def lv = DateUtils.AddDate('dd', GenerationUtils.GenerateInt(1, 50), lastValue() as Date)
                        if (lv > s2.date)
                            s2.date = lv
                        saveValue lv
                    }
                }
            }
            assertEquals(s1.count, historypoint('group:hp1').lastValue())
            assertEquals(s2.date, historypoint('group:hp2').lastValue())

            sql {
                useConnection con
                exec 'LOAD_POINT group:hp1 TO h1_value;'
                assertEquals(s1.count, vars.h1_value)
                exec 'LOAD_POINT group:hp2 TO h2_value;'
                assertEquals(s2.date, vars.h2_value)

                vars.h1_value = s1.count + 1
                exec 'SAVE_POINT group:hp1 FROM h1_value'
                assertEquals(s1.count + 1, vars.h1_value)

                vars.h2_value = DateUtils.AddDate('dd', 1, s2.date)
                exec 'SAVE_POINT group:hp2 FROM h2_value'
                assertEquals(DateUtils.AddDate('dd', 1, s2.date), vars.h2_value)
            }

            tab.drop()
        }
    }

    @Test
    void testSqlScripter() {
        Getl.Dsl(this) {
            def tab = embeddedTable {
                tableName = 'test_sqlscripter'
                field('id') { type = integerFieldType; isKey = true }
                field('name') {length = 50; isNull = false }
                create()

                etl.rowsTo {
                    writeRow { add ->
                        (1..99).each { add id: it, name: "test $it" }
                    }
                }
            }

            embeddedConnection() {
                extensionForSqlScripts = true
            }

            sql(embeddedConnection()) {
                vars.char = '-'
                vars.null_var = null
                exec'''SELECT *, '{Char}' AS test, '{null_var}' AS null_test FROM test_sqlscripter WHERE id = 1 /* ; */
-- SELECT 1;
UNION ALL -- SELECT 1;
SELECT *, '${char}' AS test, '${null_var}' AS null_test  -- Comment; 
FROM test_sqlscripter WHERE id = 2;
'''
            }

            sql(embeddedConnection()) {
                vars.list_id = [1,2,3,4,5,6]
                debugMode = true
                exec '''ECHO Test SQL scripter
CREATE TABLE test_sqlscripter_result (id int PRIMARY KEY);
ALTER TABLE test_sqlscripter_result ADD name varchar(50) NOT NULL;
CREATE INDEX test_sqlscripter_result_name ON test_sqlscripter_result (name);
COMMAND {
    DELETE FROM test_sqlscripter_result;
} 
COMMAND {
    TRUNCATE TABLE test_sqlscripter_result;
}
FOR (SELECT id, Name FROM test_sqlscripter WHERE id IN ({list_id})) DO {
    ECHO Process {id} id ...
    IF ({id} % 2 = 0) DO {
        SET SELECT {id} AS var_id, '{name}' AS var_name;
        
        /*:rows*/
        SELECT * 
        FROM test_sqlscripter 
        WHERE id = {var_id} AND name = '{var_name}';
        
        /*:count_rows*/
        INSERT INTO test_sqlscripter_result(id, name) VALUES ({var_id}, '{var_name}');
        
        IF ({count_rows} = 0) DO {
            ERROR Invalid insert {var_id}!
            ECHO Exiting ...
        }
    }
}
COMMIT;
TRUNCATE TABLE test_sqlscripter;
DROP TABLE test_sqlscripter_result;
'''
                def cmd = historyCommands.readLines()
                assertEquals('DELETE FROM test_sqlscripter_result;', cmd[0])
                assertEquals('TRUNCATE TABLE test_sqlscripter_result;', cmd[1])

                def dml = historyDML.readLines()
                assertEquals('/*:count_rows*/', dml[0])
                assertEquals('        INSERT INTO test_sqlscripter_result(id, name) VALUES (2, \'test 2\')', dml[1])
                assertEquals('/*:count_rows*/', dml[2])
                assertEquals('        INSERT INTO test_sqlscripter_result(id, name) VALUES (4, \'test 4\')', dml[3])
                assertEquals('/*:count_rows*/', dml[4])
                assertEquals('        INSERT INTO test_sqlscripter_result(id, name) VALUES (6, \'test 6\')', dml[5])
                assertEquals('COMMIT', dml[6])
                assertEquals('TRUNCATE TABLE test_sqlscripter', dml[7])

                def ddl = historyDDL.readLines()
                assertEquals('CREATE TABLE test_sqlscripter_result (id int PRIMARY KEY)', ddl[0])
                assertEquals('ALTER TABLE test_sqlscripter_result ADD name varchar(50) NOT NULL', ddl[1])
                assertEquals('CREATE INDEX test_sqlscripter_result_name ON test_sqlscripter_result (name)', ddl[2])
                assertEquals('DROP TABLE test_sqlscripter_result', ddl[3])
            }
        }
    }

    @Test
    void testSqlScripterVarCount() {
        Getl.Dsl {
            useEmbeddedConnection embeddedConnection()
            def tab = embeddedTable('#count_vars', true) {
                schemaName = 'public'
                tableName = 'count_vars_table'
                field('startdate') { type = datetimeFieldType; isKey = true }
                field('enddate') { type = datetimeFieldType; isKey = true }
                field('value') { type = integerFieldType; isNull = false }
                create()
            }

            sql {
                vars.date1 = null
                vars.date2 = null
                exec '''
SET SELECT  case when '${date1}' = '' then date_trunc('day', sysdate -30) else '${date1}' end AS datefrom;
SET SELECT  case when '${date2}'= '' then date_trunc('day', sysdate -1)  else '${date2}' end AS dateto;
ECHO Date from ${datefrom} to ${dateto}

/*
  Start processing
*/

-- Insert dates to table
/*:count_ins_dates*/
insert into public.count_vars_table (startdate, enddate, value) values ('${datefrom}', '${dateto}', 0);
ECHO Inserted ${count_ins_dates} rows to public.count_vars_table.
IF (${count_ins_dates} <> 1) DO {
  ERROR Error insert row!
}

------------------

/*:count_upd_dates*/
update public.count_vars_table set value = 1 where value = 0;
ECHO Updated ${count_upd_dates} rows from public.count_vars_table.
IF (${count_upd_dates} <> 1) DO {
  ERROR Error update row (${count_upd_dates} row detected)!
}

------------------

/*:rows*/
SELECT * FROM public.count_vars_table;
ECHO Selected rows from public.count_vars_table:\\n${rows}
------------------

/*:count_del_dates*/
delete from public.count_vars_table;
ECHO Deleted ${count_del_dates} rows from public.count_vars_table.
IF (${count_del_dates} <> 1) DO {
  ERROR Error delete row!
}

'''
                assertEquals(1, (vars.rows as List).size())
            }

            tab.drop()
        }
    }

    @Test
    void testScriptSwitchLogin() {
        Getl.Dsl {
            embeddedConnection('#test_switch_login', true) {
                executeCommand("CREATE USER test1 PASSWORD 'test1' ADMIN; CREATE USER test2 PASSWORD 'test2' ADMIN;")
                storedLogins.test1 = 'test1'
                storedLogins.test2 = 'test2'
                sql {
                    exec '''ECHO Switch login to test1 ...
SWITCH_LOGIN test1;
'''
                    assertEquals('test1', it.currentJDBCConnection.login)

                    exec '''ECHO Switch login to test2 ...
SWITCH_LOGIN test2;
'''
                    assertEquals('test2', it.currentJDBCConnection.login)
                }
                connected = false
            }
        }
    }

    @Test
    void testScriptRunFile() {
        Getl.Dsl {
            embeddedConnection('#test_run_file', true) {
                sql {
                    vars.script_param = 1
                    exec '''ECHO Run scrip.sql ...
RUN_FILE resource:/jdbc/script.sql
'''
                    assertEquals('COMPLETE', vars.script_result)
                }
            }
        }
    }

    @Test
    void testEcho() {
        Getl.Dsl {
            embeddedConnection { con ->
                def tab = embeddedTable {
                    useConnection con
                    tableName = 'test_sqlscripter_echo'
                    field('id') { type = integerFieldType; isKey = true }
                    create()
                }

                etl.rowsTo(tab) {
                    writeRow { add ->
                        add([id: 1])
                    }
                }
                assertEquals(1, tab.countRow())

                sql {
                    vars.var1 = 'test1'
                    vars.table = tab.fullTableName
                    script = '''SET SELECT Count(*) AS before_rows FROM {table};
/*
  Comment
*/
Echo {var1} -- Comment
TRUNCATE TABLE {table};

-- Comment
SET SELECT Count(*) AS after_rows FROM {table};'''
                    runSql true
                    assertEquals(1, vars.before_rows)
                    assertEquals(0, vars.after_rows)
                }

                assertEquals(0, tab.countRow())

                tab.drop()
            }
        }
    }

    @Test
    void testFormatVars() {
        Getl.Dsl {
            embeddedConnection {
                sql {
                    vars.date = DateUtils.ParseSQLDate('2023-01-31')
                    println vars.date
                    vars.time = DateUtils.ParseSQLTime('23:59:59')
                    println vars.time
                    vars.timestamp = DateUtils.ParseSQLTimestamp('2023-01-31 23:59:59.123456')
                    println vars.timestamp
                    script = '''Echo {date} {time} {timestamp}
SET SELECT 
    ('{date}' = '2023-01-31') AS date_correct, 
    ('{time}' = '23:59:59') AS time_correct,
    ('{timestamp}' = '2023-01-31 23:59:59.123456') AS timestamp_correct;
'''
                    runSql true

                    assertTrue(vars.date_correct as Boolean)
                    assertTrue(vars.time_correct as Boolean)
                    assertTrue(vars.timestamp_correct as Boolean)
                }
            }
        }
    }

    @Test
    void testCloneConnection() {
        Getl.Dsl {
            def con1 = embeddedConnection {
                login = 'user1'
                password = '1'
                storedLogins.user1 = '1'
                storedLogins.user2 = '2'
                storedLogins.user3 = '3'
            }

            def con2 = cloneConnection(con1) as TDS
            assertEquals('user1', con2.login)
            assertEquals('9D860BDC3D8A0D9144DCFE252DF7FB59', con2.password)
            assertEquals(3, con2.storedLogins.size())
            assertEquals('9D860BDC3D8A0D9144DCFE252DF7FB59', con2.storedLogins.user1)
            assertEquals('561F4D2C09DB60931C87EA14BFF904F8', con2.storedLogins.user2)
            assertEquals('333FC4C497DC27ED780FBA55CC03D2BA', con2.storedLogins.user3)
        }
    }
}