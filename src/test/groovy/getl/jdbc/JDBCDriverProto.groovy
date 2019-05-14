package getl.jdbc

import getl.data.*
import getl.driver.Driver
import getl.proc.Flow
import getl.stat.ProcessTime
import getl.tfs.TFS
import getl.utils.Config
import getl.utils.DateUtils
import getl.utils.FileUtils
import getl.utils.GenerationUtils
import getl.utils.Logs
import getl.utils.NumericUtils
import getl.utils.StringUtils
import groovy.transform.CompileStatic
import groovy.transform.InheritConstructors

import java.sql.Time

/**
 * Created by ascru on 21.11.2016.
 */
@InheritConstructors
abstract class JDBCDriverProto extends getl.test.GetlTest {
	def static configName = 'tests/jdbc/setup.conf'

    @Override
	void setUp() {
        super.setUp()
		if (!FileUtils.ExistsFile(configName)) return
		Config.LoadConfig(fileName: configName)
		Logs.Init()
	}

    private static final countRows = 100
    private JDBCConnection _con
    protected String defaultSchema
    abstract protected JDBCConnection newCon()
    public JDBCConnection getCon() {
        if (_con == null) _con = newCon()
        return _con
    }
    final def table = new TableDataset(connection: con, schemaName: defaultSchema, tableName: '_getl_test')
    List<Field> getFields () {
        def res =
            [
                new Field(name: 'ID1', type: 'BIGINT', isKey: true, ordKey: 1),
                new Field(name: 'ID2', type: 'DATETIME', isKey: true, ordKey: 2),
                new Field(name: 'Name', type: 'STRING', length: 50, isNull: false),
				new Field(name: "desc'ription", type: 'STRING', length: 250, isNull: false),
                new Field(name: 'value', type: 'NUMERIC', length: 12, precision: 2, isNull: false),
                new Field(name: 'DOuble', type: 'DOUBLE', isNull: false, defaultValue: 0),
            ]

		if (con != null && con.driver.isSupport(Driver.Support.DATE)) res << new Field(name: 'date', type: 'DATE', isNull: false)
		if (con != null && con.driver.isSupport(Driver.Support.BOOLEAN)) res << new Field(name: 'flag', type: 'BOOLEAN', isNull: false)
		if (con != null && con.driver.isSupport(Driver.Support.TIME)) res << new Field(name: 'time', type: 'TIME', isNull: false)
        if (con != null && con.driver.isSupport(Driver.Support.BLOB)) res << new Field(name: 'data', type: 'BLOB', length: 1024, isNull: false)
        if (con != null && con.driver.isSupport(Driver.Support.CLOB)) res << new Field(name: 'text', type: 'TEXT', length: 1024, isNull: false)
		if (con != null && con.driver.isSupport(Driver.Support.UUID)) res << new Field(name: 'uniqueid', type: 'UUID', isNull: false)

        return res
    }

    @Override
    protected void runTest() {
        if (con != null) super.runTest()
    }

    void connect() {
        con.connected = true
        assertTrue(con.connected)
    }

    void disconnect() {
        con.connected = false
        assertFalse(con.connected)
    }

    private void createTable() {
        table.field = fields
        if (table.exists) table.drop()
        if (con.driver.isSupport(Driver.Support.INDEX)) {
			def indexes = [
					_getl_test_idx_1:
							[columns: ['id2', 'name']]]
			if (con != null && con.driver.isSupport(Driver.Support.DATE))
				indexes << [_getl_test_idx_2: [columns: ['id1', 'date'], unique: true]]
            table.create(indexes: indexes)
        }
        else {
            table.create()
        }

        assertTrue(table.exists)
    }

    public void testLocalTable() {
        if (!con.driver.isSupport(Driver.Support.LOCAL_TEMPORARY)) {
            println "Skip test local temporary table: ${con.driver.getClass().name} not support this futures"
            return
        }
        def tempTable = new TableDataset(connection: con, tableName: '_getl_local_temp_test', type: JDBCDataset.Type.LOCAL_TEMPORARY)
        tempTable.field = fields
        if (con.driver.isSupport(Driver.Support.INDEX)) {
            tempTable.create(indexes: [_getl_local_temp_test_idx_1: [columns: ['id2', 'name']]])
        }
        else {
            tempTable.create()
        }
        tempTable.drop()
    }

    public void testGlobalTable() {
        if (!con.driver.isSupport(Driver.Support.GLOBAL_TEMPORARY)) {
            println "Skip test global temporary table: ${con.driver.getClass().name} not support this futures"
            return
        }
        def tempTable = new TableDataset(connection: con, tableName: '_getl_global_temp_test', type: JDBCDataset.Type.GLOBAL_TEMPORARY)
        tempTable.field = fields
//        tempTable.drop(ifExists: true)
        if (con.driver.isSupport(Driver.Support.INDEX)) {
            tempTable.create(indexes: [_getl_global_temp_test_idx_1: [columns: ['id2', 'name']]])
        }
        else {
            tempTable.create()
        }
        tempTable.drop()
    }

    private void dropTable() {
        table.drop()
        assertFalse(table.exists)
    }

    private void retrieveFields() {
        table.field.clear()
        table.retrieveFields()

		def origFields = [] as List<Field>
		fields.each {Field of ->
			def f = of.copy()
			origFields << f

			f.name = f.name.toLowerCase()
			f.defaultValue = null

			if (f.type == Field.Type.BIGINT && (con.driver as JDBCDriver).sqlType.BIGINT.name.toUpperCase() == 'NUMBER') {
				f.type = Field.Type.NUMERIC
				f.length = null
			}

			if (f.type != Field.Type.NUMERIC) {
				f.precision = null
			}

			if (!(f.type in [Field.Type.STRING, Field.Type.NUMERIC])) {
				if ((f.type != Field.Type.TEXT || (con.driver as JDBCDriver).sqlType.TEXT.useLength == JDBCDriver.sqlTypeUse.NEVER) &&
						(f.type != Field.Type.BLOB || (con.driver as JDBCDriver).sqlType.BLOB.useLength == JDBCDriver.sqlTypeUse.NEVER))
					f.length = null
			}

			if (f.type == Field.Type.TEXT) f.type = Field.Type.STRING

		}
		def dsFields = [] as List<Field>
		table.field.each {Field of ->
			def f = of.copy()
			dsFields << f

			f.name = f.name.toLowerCase()
			f.defaultValue = null
			f.dbType = null
			f.typeName = null

			if (f.type != Field.Type.NUMERIC) {
				f.precision = null
			}

			if (!(f.type in [Field.Type.STRING, Field.Type.NUMERIC])) {
				if ((f.type != Field.Type.TEXT || (con.driver as JDBCDriver).sqlType.TEXT.useLength == JDBCDriver.sqlTypeUse.NEVER) &&
						(f.type != Field.Type.BLOB || (con.driver as JDBCDriver).sqlType.BLOB.useLength == JDBCDriver.sqlTypeUse.NEVER))
					f.length = null
			}

			if (f.type == Field.Type.TEXT) f.type = Field.Type.STRING
		}

        assertEquals(origFields, dsFields)
    }

    private void insertData() {
        def count = new Flow().writeTo(dest: table) { updater ->
            (1..countRows).each { num ->
                Map r = GenerationUtils.GenerateRowValues(table.field, num)
                r.id1 = num

                updater(r)
            }
        }
        assertEquals(countRows, count)
        validCount()

        def counter = 9
		table.eachRow(order: ['id1'], limit: 10, offs: 10) { r ->
            counter++
			assertEquals(counter, r.id1)
			assertNotNull(r.id2)
			assertNotNull(r.name)
			assertNotNull(r."desc'ription")
			assertNotNull(r.value)
			assertNotNull(r.double)
			if (con.driver.isSupport(Driver.Support.DATE)) assertNotNull(r.date)
			if (con.driver.isSupport(Driver.Support.TIME)) assertNotNull(r.time)
			if (con.driver.isSupport(Driver.Support.BOOLEAN)) assertNotNull(r.flag)
			if (con.driver.isSupport(Driver.Support.CLOB)) assertNotNull(r.text)
			if (con.driver.isSupport(Driver.Support.BLOB)) assertNotNull(r.data)
			if (con.driver.isSupport(Driver.Support.UUID)) assertNotNull(r.uniqueid)
		}
        assertEquals(10, table.readRows)
    }

    private void updateData() {
        def rows = table.rows(order: ['id1'])
        def count = new Flow().writeTo(dest: table, dest_operation: 'UPDATE') { updater ->
            rows.each { r ->
                Map nr = [:]
                nr.putAll(r)
                nr.name = StringUtils.LeftStr(r.name, 40) + ' update'
				nr."desc'ription" = StringUtils.LeftStr(r."desc'ription", 200) + ' update'
                nr.value = r.value + 1
                nr.double = r.double + 1.00
				if (con.driver.isSupport(Driver.Support.DATE)) nr.date = DateUtils.AddDate('dd', 1, r.date)
				if (con.driver.isSupport(Driver.Support.TIME)) nr.time = java.sql.Time.valueOf((r.time as java.sql.Time).toLocalTime().plusSeconds(100))
				if (con.driver.isSupport(Driver.Support.BOOLEAN)) nr.flag = GenerationUtils.GenerateBoolean()
				if (con.driver.isSupport(Driver.Support.CLOB)) nr.text = GenerationUtils.GenerateString(1024)
				if (con.driver.isSupport(Driver.Support.BLOB)) nr.data = GenerationUtils.GenerateString(512).bytes
				if (con.driver.isSupport(Driver.Support.UUID)) nr.uniqueid = UUID.randomUUID().toString()

                updater(nr)
            }
        }
        assertEquals(countRows, count)
        validCount()

		def i = 0
        table.eachRow(order: ['id1']) { r ->
            assertEquals(StringUtils.LeftStr(rows[i].name, 40) + ' update', r.name)
			assertEquals(StringUtils.LeftStr(rows[i]."desc'ription", 200) + ' update', r."desc'ription")
            assertEquals(rows[i].value + 1, r.value)
			assertNotNull(r.double)
			if (con.driver.isSupport(Driver.Support.DATE)) assertEquals(DateUtils.AddDate('dd', 1, rows[i].date), r.date)
			if (con.driver.isSupport(Driver.Support.TIME)) assertEquals(java.sql.Time.valueOf((rows[i].time as java.sql.Time).toLocalTime().plusSeconds(100)), r.time)
			if (con.driver.isSupport(Driver.Support.BOOLEAN)) assertNotNull(r.flag)
			if (con.driver.isSupport(Driver.Support.CLOB)) assertNotNull(r.text)
			if (con.driver.isSupport(Driver.Support.BLOB)) assertNotNull(r.data)
			if (con.driver.isSupport(Driver.Support.UUID)) assertNotNull(r.uniqueid)

			i++
        }
    }

    private void mergeData() {
        def rows = table.rows(order: ['id1'])
        def count = new Flow().writeTo(dest: table, dest_operation: 'MERGE') { updater ->
            rows.each { r ->
                Map nr = [:]
                nr.putAll(r)
                nr.name = StringUtils.LeftStr(r.name, 40) + ' merge'
				nr."desc'ription" = StringUtils.LeftStr(r."desc'ription", 200) + ' merge'
                nr.value = r.value + 1
                nr.double = r.double + 1.00
				if (con.driver.isSupport(Driver.Support.DATE)) nr.date = DateUtils.AddDate('dd', 1, r.date)
				if (con.driver.isSupport(Driver.Support.TIME)) nr.time = java.sql.Time.valueOf((r.time as java.sql.Time).toLocalTime().plusSeconds(100))
				if (con.driver.isSupport(Driver.Support.BOOLEAN)) nr.flag = GenerationUtils.GenerateBoolean()
				if (con.driver.isSupport(Driver.Support.CLOB)) nr.text = GenerationUtils.GenerateString(1024)
				if (con.driver.isSupport(Driver.Support.BLOB)) nr.data = GenerationUtils.GenerateString(512).bytes
				if (con.driver.isSupport(Driver.Support.UUID)) nr.uniqueid = UUID.randomUUID().toString()

                updater(nr)
            }
        }
        assertEquals(countRows, count)
        validCount()

		def i = 0
        table.eachRow(order: ['id1']) { r ->
            assertEquals(StringUtils.LeftStr(rows[i].name, 40) + ' merge', r.name)
			assertEquals(StringUtils.LeftStr(rows[i]."desc'ription", 200) + ' merge', r."desc'ription")
            assertEquals(rows[i].value + 1, r.value)
			assertNotNull(r.double)
			if (con.driver.isSupport(Driver.Support.DATE)) assertEquals(DateUtils.AddDate('dd', 1, rows[i].date), r.date)
			if (con.driver.isSupport(Driver.Support.TIME)) assertEquals(java.sql.Time.valueOf((rows[i].time as java.sql.Time).toLocalTime().plusSeconds(100)), r.time)
			if (con.driver.isSupport(Driver.Support.BOOLEAN)) assertNotNull(r.flag)
			if (con.driver.isSupport(Driver.Support.CLOB)) assertNotNull(r.text)
			if (con.driver.isSupport(Driver.Support.BLOB)) assertNotNull(r.data)
			if (con.driver.isSupport(Driver.Support.UUID)) assertNotNull(r.uniqueid)

			i++
        }
    }

    private void validCount() {
        def q = new QueryDataset(connection: con, query: "SELECT Count(*) AS count_rows FROM ${table.fullNameDataset()} WHERE ${table.sqlObjectName('name')} IS NOT NULL AND ${table.sqlObjectName("desc'ription")} IS NOT NULL")
        def rows = q.rows()
        assertEquals(1, rows.size())
        assertEquals(countRows, rows[0].count_rows)
    }

    private void deleteData() {
        def rows = table.rows(onlyFields: ['id1', 'id2'])
        def count = new Flow().writeTo(dest: table, dest_operation: 'DELETE') { updater ->
            rows.each { r ->
                updater(r)
            }
        }
        assertEquals(countRows, count)
        validCountZero()
    }

    private void truncateData() {
        table.truncate(truncate: true)
    }

    private void queryData() {
        def drv = (con.driver as JDBCDriver)
        def fieldName = drv.prepareFieldNameForSQL('ID1', table)

        def q1 = new QueryDataset(connection: con, query: "SELECT * FROM ${table.objectFullName} WHERE $fieldName = :param1")
        def r1 = q1.rows(sqlParams: [param1: 1])
        assertEquals(1, r1.size())
        assertEquals(1, r1[0].id1)

        def q2 = new QueryDataset(connection: con, query: "SELECT * FROM ${table.objectFullName} WHERE $fieldName = {param1}")
        def r2 = q2.rows(queryParams: [param1: 2])
        assertEquals(1, r2.size())
        assertEquals(2, r2[0].id1)
    }

    private void validCountZero() {
        def q = new QueryDataset(connection: con, query: "SELECT Count(*) AS count_rows FROM ${table.fullNameDataset()}")
        def rows = q.rows()
        assertEquals(1, rows.size())
        assertEquals(0, rows[0].count_rows)
    }

    private void runCommandUpdate() {
        con.startTran()
        def count = con.executeCommand(command: "UPDATE ${table.fullNameDataset()} SET ${table.sqlObjectName('double')} = ${table.sqlObjectName('double')} + 1", isUpdate: true)
        assertEquals(countRows, count)
        con.commitTran()
        def q = new QueryDataset(connection: con, query: "SELECT Count(*) AS count_rows FROM ${table.fullNameDataset()} WHERE ${table.sqlObjectName('double')} IS NULL")
        def rows = q.rows()
        assertEquals(1, rows.size())
    }

    private void bulkLoad() {
        def file = TFS.dataset()
        def count = new Flow().copy(source: table, dest: file, inheritFields: true)
        assertEquals(countRows, count)
        truncateData()
        validCountZero()
        table.bulkLoadFile(source: file)
        validCount()
    }

    private void runScript() {
        def table_name = table.fullNameDataset()
        def sql = """
----- Test scripter
ECHO Run sql script ...
IF EXISTS(SELECT * FROM $table_name); -- test IF operator
ECHO Table has rows -- test ECHO 
END IF;
SET SELECT ${table.sqlObjectName('id2')} FROM $table_name WHERE ${table.sqlObjectName('id1')} = 1; -- test SET operator
ECHO For id1=1 then id2={id2}

FOR SELECT ${table.sqlObjectName('id1')}, ${table.sqlObjectName('id2')} FROM $table_name WHERE ${table.sqlObjectName('id1')} BETWEEN 2 AND 3; -- test FOR operator
ECHO For id1={id1} then id2={id2}
END FOR;
"""
        def scripter = new SQLScripter(connection: table.connection, script: sql)
        scripter.runSql()
    }

    public void testOperations() {
        connect()
        createTable()
        retrieveFields()
        insertData()
        updateData()
        queryData()
        if (con.driver.isOperation(Driver.Operation.MERGE)) {
            mergeData()
        }
        if (con.driver.isOperation(Driver.Operation.BULKLOAD)) {
            bulkLoad()
        }
        runCommandUpdate()
        runScript()
        deleteData()
        dropTable()
        disconnect()
    }

	@CompileStatic
	public void testPerfomance() {
		def c = newCon()
		if (c == null) return
		if (Config.content.perfomanceRows == null) return
		def perfomanceRows = Config.content.perfomanceRows as Integer
		def perfomanceCols = (Config.content.perfomanceCols as Integer)?:100
		Logs.Finest("Test ${c.driverName} perfomance write from $perfomanceRows rows with ${perfomanceCols+2} cols ...")
		TableDataset t = new TableDataset(connection: c, tableName: '_GETL_TEST_PERFOMANCE')
		t.field << new Field(name: 'id', type: Field.Type.INTEGER, isKey: true)
		t.field << new Field(name: 'name', length: 50, isNull: false)
		t.field << new Field(name: 'desc\'cription', length: 50, isNull: false)
		(1..perfomanceCols).each { num ->
			t.field << new Field(name: "value_$num", type: Field.Type.DOUBLE)
		}
		if (t.exists) t.drop()
		t.create()
		try {
			def pt = new ProcessTime(name: "${c.driverName} perfomance write")
			new Flow().writeTo(dest: t, dest_batchSize: 1000) { Closure updater ->
				(1..perfomanceRows).each { Integer cur ->
					cur++
					def r = [:] as Map<String, Object>
					r.id = cur
					r.name = "name $cur"
					r."desc'cription" = "description $cur"
					(1..perfomanceCols).each { Integer num ->
						r.put("value_$num".toString(), cur)
					}
					updater(r)
				}
			}
			pt.finish(perfomanceRows as Long)

			pt = new ProcessTime(name: "${c.driverName} perfomance read")
			def count = 0
			new Flow().process(source: t) { Map<String, Object> r ->
				count++
			}
			pt.finish(count as Long)
		}
		finally {
			t.drop()
		}
	}
}