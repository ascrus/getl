package getl.jdbc

import getl.data.*
import getl.driver.Driver
import getl.proc.Flow
import getl.stat.ProcessTime
import getl.test.GetlTest
import getl.tfs.TFS
import getl.utils.*
import groovy.transform.InheritConstructors
import org.junit.Before
import org.junit.BeforeClass
import org.junit.Test

import java.sql.Timestamp

/**
 * Created by ascru on 21.11.2016.
 */
@InheritConstructors
abstract class JDBCDriverProto extends GetlTest {
	def static configName = 'resource:/jdbc/setup.conf'

    @BeforeClass
	static void InitTest() {
		Config.LoadConfig(fileName: configName)
	}

    static final countRows = 100L
    JDBCConnection _con
    static final def validConnections = [:]
    protected String defaultDatabase
    protected String defaultSchema
    protected JDBCConnection newCon() {
        return null
    }

    String getDescriptionName() { "desc'ription" }

    JDBCConnection getCon() {
        if (_con == null) {
            def c = newCon()
            if (c != null) {
                if (!validConnections.containsKey(c.getClass().name)) {
                    try {
                        c.connected = true
                        c.connected = false
                        validConnections.put(c.getClass().name, true)
                        _con = c
                    }
                    catch (Exception e) {
                        validConnections.put(c.getClass().name, false)
                        Logs.Exception(e)
                    }
                }
                else {
                    if (validConnections.get(c.getClass().name) == true)
                        _con = c
                }
            }
        }

        return _con
    }

    String getUseTableName() { 'getl_test_data' }
    TableDataset table
    List<Field> getFields () {
        def res =
            [
                new Field(name: 'ID1', type: 'BIGINT', isKey: true, ordKey: 1),
                new Field(name: 'ID2', type: 'DATETIME', isKey: true, ordKey: 2),
                new Field(name: 'Name', type: 'STRING', length: 50, isNull: false),
				new Field(name: descriptionName, type: 'STRING', length: 250, isNull: false),
                new Field(name: 'value', type: 'NUMERIC', length: 12, precision: 2, isNull: false),
                new Field(name: 'DOuble', type: 'DOUBLE', isNull: false, defaultValue: 0),
            ]

		if (con != null && useDate) res << new Field(name: 'date', type: 'DATE', isNull: false)
		if (con != null && useTime) res << new Field(name: 'time', type: 'TIME', isNull: false)
        if (con != null && useTimestampWithZone) res << new Field(name: 'dtwithtz', type: 'TIMESTAMP_WITH_TIMEZONE', isNull: false)
        if (con != null && useBoolean) res << new Field(name: 'flag', type: 'BOOLEAN', isNull: false)
        if (con != null && useClob) res << new Field(name: 'text', type: 'TEXT', length: 1024)
        if (con != null && useBlob) res << new Field(name: 'data', type: 'BLOB', length: 1024)
		if (con != null && useUuid) res << new Field(name: 'uniqueid', type: 'UUID', isNull: false)
        if (con != null && useArray) res << new Field(name: 'list', type: 'ARRAY', isNull: false, arrayType: 'INT8')

        return res
    }

    @Override
    Boolean allowTests() {
        return (con != null)
    }

    void prepareTable() { }

    @Before
    void initTable() {
        if (con != null) {
            table = con.newDataset() as TableDataset
            table.tableName = useTableName
            prepareTable()
        }
    }

    protected void connect() {
        con.connected = true
    }

    protected void disconnect() {
        con.connected = false
    }

    protected void createTable() {
        table.field = fields
        table.drop(ifExists: true)
        if (con.driver.isSupport(Driver.Support.INDEX)) {
			def indexes = [:]
            indexes.put("${table.tableName}_idx_1".toString(), [columns: ['id2', 'name']])
			if (con != null && useDate)
				indexes.put("${table.tableName}_idx_2".toString(), [columns: ['id1', 'date'], unique: true])
            table.create(ifNotExists: true, indexes: indexes)
        }
        else {
            table.create()
        }

        assertTrue(table.exists)

        table.field('name').isNull = false
        table.field(descriptionName).isNull = false
    }

    private Sequence sequence
    protected Sequence getSequence() { sequence }
    protected boolean getTestSequence() { true }

    protected void createSequence() {
        if (!con.driver.isSupport(Driver.Support.SEQUENCE) || !testSequence) return
        sequence = new Sequence()
        sequence.tap {
            useConnection con
            name = 'getl_test_sequence'
            dropSequence(true)
            cache = 10
            createSequence(true) {
                incrementBy = 10
                cacheNumbers = countRows
            }
        }
    }

    @Test
    void testLocalTable() {
        if (!con.driver.isSupport(Driver.Support.LOCAL_TEMPORARY)) {
            println "Skip test local temporary table: ${con.driver.getClass().name} not support this futures"
            return
        }
        def tempTable = new TableDataset(connection: con, /*schemaName: '_getl_test',*/
                tableName: '_getl_local_temp_test', type: JDBCDataset.Type.LOCAL_TEMPORARY)
        tempTable.field = fields
        if (con.driver.isSupport(Driver.Support.INDEX)) {
            tempTable.create(indexes: [_getl_local_temp_test_idx_1: [columns: ['id2', 'name']]])
        }
        else {
            tempTable.create()
        }

        if (tempTable.currentJDBCConnection.currentJDBCDriver.supportLocalTemporaryRetrieveFields) {
            tempTable.field = null
            tempTable.retrieveFields()
            assertEquals(fields*.name*.toLowerCase(), tempTable.field*.name*.toLowerCase())
        }

        tempTable.drop()
    }

    @Test
    void testGlobalTable() {
        if (!con.driver.isSupport(Driver.Support.GLOBAL_TEMPORARY)) {
            println "Skip test global temporary table: ${con.driver.getClass().name} not support this futures"
            return
        }
        def tempTable = new TableDataset(connection: con, tableName: '_getl_global_temp_test', type: JDBCDataset.Type.GLOBAL_TEMPORARY)
        tempTable.field = fields
        tempTable.drop(ifExists: true)
        if (con.driver.isSupport(Driver.Support.INDEX)) {
            tempTable.create(indexes: [_getl_global_temp_test_idx_1: [columns: ['id2', 'name']]])
        }
        else {
            tempTable.create()
        }

        tempTable.field = null
        tempTable.retrieveFields()
        assertEquals(fields*.name*.toLowerCase(), tempTable.field*.name*.toLowerCase())

        tempTable.drop()
    }

    protected void dropTable() {
        table.drop(ifExists: true)
        assertFalse(table.exists)
    }

    protected void retrieveFields() {
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

            if (f.type == Field.stringFieldType && (con.driver as JDBCDriver).sqlType.STRING.useLength == JDBCDriver.sqlTypeUse.NEVER) {
                f.length = null
            }

			if (!(f.type in [Field.Type.STRING, Field.Type.NUMERIC])) {
				if ((f.type != Field.Type.TEXT || (con.driver as JDBCDriver).sqlType.TEXT.useLength == JDBCDriver.sqlTypeUse.NEVER) &&
						(f.type != Field.Type.BLOB || (con.driver as JDBCDriver).sqlType.BLOB.useLength == JDBCDriver.sqlTypeUse.NEVER))
					f.length = null
			}

			if (f.type == Field.Type.TEXT) f.type = Field.Type.STRING

            if (!(Driver.Support.PRIMARY_KEY in table.connection.driver.supported()) && f.isKey) {
                f.isKey = false
                f.isNull = true
                f.ordKey = null
            }

            if (!(Driver.Support.NOT_NULL_FIELD in table.connection.driver.supported()) && !f.isNull) {
                f.isNull = true
            }
		}

		def dsFields = [] as List<Field>
		table.field.each { of ->
			def f = of.copy()
			dsFields << f

			f.name = f.name.toLowerCase()
			f.defaultValue = null
			f.dbType = null
			f.typeName = null

			if (f.type != Field.Type.NUMERIC) {
				f.precision = null
			}

            if (f.type == Field.stringFieldType && (con.driver as JDBCDriver).sqlType.STRING.useLength == JDBCDriver.sqlTypeUse.NEVER) {
                f.length = null
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

    protected void retrieveObject() {
        def d = con.retrieveDatasets { tableMask = [table.tableName] }
        assertEquals(1, d.size())
        def l = con.retrieveObjects(tableMask: table.tableName)
        assertEquals(1, l.size())
    }

    protected boolean getUseDate() { con.driver.isSupport(Driver.Support.DATE) }
    protected boolean getUseTime() { con.driver.isSupport(Driver.Support.TIME) }
    protected boolean getUseTimestampWithZone() { con.driver.isSupport(Driver.Support.TIMESTAMP_WITH_TIMEZONE) }
    protected boolean getUseBoolean() { con.driver.isSupport(Driver.Support.BOOLEAN) }
    protected boolean getUseClob() { con.driver.isSupport(Driver.Support.CLOB) }
    protected boolean getUseBlob() { con.driver.isSupport(Driver.Support.BLOB) }
    protected boolean getUseUuid() { con.driver.isSupport(Driver.Support.UUID) }
    protected boolean getUseArray() { con.driver.isSupport(Driver.Support.ARRAY) }

    protected long insertData() {
        if (!con.driver.isOperation(Driver.Operation.INSERT)) return 0

        createSequence()

        def count = new Flow().writeTo(dest: table) { updater ->
            (1..countRows).each { num ->
                Map r = GenerationUtils.GenerateRowValues(table.field, table.currentJDBCConnection.currentJDBCDriver.lengthTextInBytes, num)
                if(this.sequence != null)
                    r.id1 = this.sequence.nextValue

                updater(r)
            }
        }
        assertEquals(countRows, count)
        validCount()

        /*con.sqlConnection.eachRow("SELECT * FROM ${table.fullNameDataset()}".toString()) {
            println it
        }*/

        def counter = 0
		table.eachRow(order: ['id1']) { r ->
            counter++
            Integer id1 = r.id1
			assertEquals(counter, id1)
			assertNotNull(r.id2)
			assertNotNull(r.name)
			assertNotNull(r.get(descriptionName))
			assertNotNull(r.value)
			assertNotNull(r.double)
			if (useDate) assertNotNull(r.date)
			if (useTime) assertNotNull(r.time)
            if (useTimestampWithZone) assertNotNull(r.dtwithtz)
			if (useBoolean) assertNotNull(r.flag)
			if (useUuid) assertNotNull(r.uniqueid)
		}
        assertEquals(countRows, table.readRows)

        return count
    }

    protected void updateData() {
        if (!con.driver.isOperation(Driver.Operation.UPDATE)) return
        def rows = table.rows(order: ['id1'])
        def i = 0
        def count = new Flow().writeTo(dest: table, dest_operation: 'UPDATE') { updater ->
            rows.each { r ->
                Map nr = [:]
                nr.putAll(r)
                nr.name = StringUtils.LeftStr(r.name, 40) + ' update'
				nr.put(descriptionName, StringUtils.LeftStr(r."desc'ription", 200) + ' update')
                nr.value = r.value + 1
                nr.double = r.double + 1.00
				if (useDate) nr.date = DateUtils.AddDate('dd', 1, r.date)
				if (useTime) nr.time = java.sql.Time.valueOf((r.time as java.sql.Time).toLocalTime().plusSeconds(100))
                if (useTimestampWithZone) nr.dtwithtz = DateUtils.AddDate('dd', 1, r.dtwithtz)
				if (useBoolean) nr.flag = GenerationUtils.GenerateBoolean()
				if (useClob) nr.text = GenerationUtils.GenerateString(128)
				if (useBlob) nr.data = GenerationUtils.GenerateString(128).bytes
				if (useUuid) nr.uniqueid = UUID.randomUUID().toString()

                updater(nr)
                rows[i] = nr
                i++
            }
        }
        assertEquals(countRows, count)
        validCount()

		i = 0
        table.eachRow(order: ['id1']) { r ->
            assertEquals("id: $r.id1".toString(), rows[i].name, r.name)
			assertEquals("id: $r.id1".toString(), rows[i].descriptionName, r.descriptionName)
            assertEquals("id: $r.id1".toString(), rows[i].value, r.value)
            assertNotNull("id: $r.id1".toString(), r.double)
			if (useDate) assertEquals("id: $r.id1".toString(), rows[i].date, r.date)
			if (useTime) assertEquals("id: $r.id1".toString(), rows[i].time.toString(), r.time.toString())
            /* TODO: incorrect work for Oracle! */
//            if (useTimestampWithZone) assertEquals("id: $r.id1".toString(), rows[i].dtwithtz, r.dtwithtz)
           if (useTimestampWithZone) assertNotNull("id: $r.id1".toString(), r.dtwithtz)
           if (useBoolean) assertEquals("id: $r.id1".toString(), rows[i].flag, r.flag)
           /*if (useClob) assertNotNull(r.text)
           if (useBlob) assertNotNull(r.data)*/
			if (useUuid) assertEquals("id: $r.id1".toString(), rows[i].uniqueid.toString().toLowerCase(), r.uniqueid.toString().toLowerCase())

			i++
        }
    }

    protected void mergeData() {
        if (!con.driver.isOperation(Driver.Operation.MERGE)) return
        def rows = table.rows(order: ['id1'])
        def count = new Flow().writeTo(dest: table, dest_operation: 'MERGE') { updater ->
            rows.each { r ->
                Map nr = [:]
                nr.putAll(r)
                nr.name = StringUtils.LeftStr(r.name, 40) + ' merge'
				nr.put(descriptionName, StringUtils.LeftStr(r."desc'ription", 200) + ' merge')
                nr.value = r.value + 1
                nr.double = r.double + 1.00
				if (useDate) nr.date = DateUtils.AddDate('dd', 1, r.date)
				if (useTime) nr.time = java.sql.Time.valueOf((r.time as java.sql.Time).toLocalTime().plusSeconds(100))
                if (useTimestampWithZone) nr.dtwithtz = DateUtils.AddDate('dd', 1, r.dtwithtz)
				if (useBoolean) nr.flag = GenerationUtils.GenerateBoolean()
				if (useClob) nr.text = GenerationUtils.GenerateString(1024)
				if (useBlob) nr.data = GenerationUtils.GenerateString(512).bytes
				if (useUuid) nr.uniqueid = UUID.randomUUID().toString()

                updater(nr)
            }
        }
        assertEquals(countRows, count)
        validCount()

		def i = 0
        table.eachRow(order: ['id1']) { r ->
            assertEquals(StringUtils.LeftStr(rows[i].name, 40) + ' merge', r.name)
			assertEquals(StringUtils.LeftStr(rows[i].get(descriptionName), 200) + ' merge', r.get(descriptionName))
            assertEquals(rows[i].value + 1, r.value)
			assertNotNull(r.double)
			if (useDate) assertEquals(DateUtils.AddDate('dd', 1, rows[i].date), r.date)
			if (useTime) assertEquals(java.sql.Time.valueOf((rows[i].time as java.sql.Time).toLocalTime().plusSeconds(100)), r.time)
            if (useTimestampWithZone) assertEquals(DateUtils.AddDate('dd', 1, rows[i].dtwithtz), r.dtwithtz)
			if (useBoolean) assertNotNull(r.flag)
			/*if (useClob) assertNotNull(r.text)
			if (useBlob) assertNotNull(r.data)*/
			if (useUuid) assertNotNull(r.uniqueid)

			i++
        }
    }

    protected void validCount() {
        def q = new QueryDataset(connection: con, query: "SELECT Count(*) AS count_rows FROM ${table.fullNameDataset()} WHERE ${table.sqlObjectName('name')} IS NOT NULL AND ${table.sqlObjectName(descriptionName)} IS NOT NULL")
        q.retrieveFields()
        assertEquals(1, q.field.size())
        assertEquals(['COUNT_ROWS'], q.fieldNames*.toUpperCase())
        def rows = q.rows()
        assertEquals(1, rows.size())
        def cr = rows[0].count_rows as Long
        assertEquals(countRows, cr)
    }

    protected void deleteData() {
        if (!con.driver.isOperation(Driver.Operation.DELETE)) return
        def rows = table.rows(onlyFields: ['id1', 'id2'])
        def count = new Flow().writeTo(dest: table, dest_operation: 'DELETE') { updater ->
            rows.each { r ->
                updater(r)
            }
        }
        assertEquals(countRows, count)
        validCountZero()
    }

    protected void truncateData() {
        if (!(con.driver.isOperation(Driver.Operation.DELETE) || con.driver.isOperation(Driver.Operation.TRUNCATE))) return

        if (table.countRow() == 0) insertData()
        truncateTable(table)
        assertEquals(0, table.countRow())
    }

    private truncateTable(TableDataset source) {
        if (!(con.driver.isOperation(Driver.Operation.DELETE) || con.driver.isOperation(Driver.Operation.TRUNCATE))) return
        source.truncate(truncate: con.driver.isOperation(Driver.Operation.TRUNCATE))
    }

    protected void deleteRows() {
        if (!con.driver.isOperation(Driver.Operation.DELETE)) return

        if (table.countRow() == 0) insertData()

        def drv = (con.driver as JDBCDriver)
        def fieldName = drv.prepareFieldNameForSQL('ID1', table)

        table.deleteRows()
        assertEquals(0, table.countRow())

        insertData()
        table.deleteRows("$fieldName > 1")
        assertEquals(1, table.countRow())

        table.deleteRows()
        assertEquals(0, table.countRow())

        insertData()
        table.writeOpts { where = "$fieldName > 1"; batchSize = 500 }
        table.deleteRows()
        assertEquals(1, table.countRow())
        table.writeDirective.clear()
        table.deleteRows()
        assertEquals(0, table.countRow())
    }

    protected void queryData() {
        def drv = (con.driver as JDBCDriver)
        def fieldName = drv.prepareFieldNameForSQL('ID1', table)

        def q1 = new QueryDataset(connection: con, query: "SELECT $fieldName FROM ${table.objectFullName} WHERE $fieldName = :param1")
        def r1 = q1.rows(sqlParams: [param1: 1])
        assertEquals(1, r1.size())
        def id1 = r1[0].id1 as Integer
        assertEquals(1, id1)

        def dt = new Timestamp(new Date().time)
        def dtExpr = drv.sqlExpression('convertTextToTimestamp')
        def q2 = new QueryDataset(connection: con, query: "SELECT $fieldName, $dtExpr AS calc_dt FROM ${table.objectFullName} WHERE $fieldName = {param1}")
        def r2 = q2.rows(queryParams: [param1: 2, value: dt])
        assertEquals(1, r2.size())
        def id2 = r2[0].id1 as Integer
        assertEquals(2, id2)
        if (dt != r2[0].calc_dt)
            assertEquals(DateUtils.TruncTime('MINUTE', dt), DateUtils.TruncTime('MINUTE', r2[0].calc_dt as Date))

        def q3 = new QueryDataset(connection: con)
        q3.loadFile('resource:/sql/test_query.sql')
        def r3 = q3.rows(queryParams: [table: table.objectFullName, field: fieldName, param1: 2])
        assertEquals(1, r3.size())
        def id3 = r3[0].id1 as Integer
        assertEquals(2, id3)

        def q4 = new QueryDataset(connection: con)
        q4.scriptFilePath = 'resource:/sql/test_query.sql'
        def r4 = q4.rows(queryParams: [table: table.objectFullName, field: fieldName, param1: 2])
        assertEquals(1, r4.size())
        def id4 = r4[0].id1 as Integer
        assertEquals(2, id4)

        def tabRows = table.select("SELECT Min($fieldName) AS min_id, Max($fieldName) AS max_id FROM {table} WHERE $fieldName > {start}", [start: 0])
        assertEquals(1, tabRows.size())
        assertEquals(1, (tabRows[0].min_id as Number).toInteger())
        assertEquals(countRows, (tabRows[0].max_id as Number).toInteger())
    }

    protected void validCountZero() {
        def q = new QueryDataset(connection: con, query: "SELECT Count(*) AS count_rows FROM ${table.fullNameDataset()}")
        def rows = q.rows()
        assertEquals(1, rows.size())
        def cr = rows[0].count_rows as Integer
        assertEquals(0, cr)
    }

    protected void runCommandUpdate() {
        if (!con.driver.isOperation(Driver.Operation.UPDATE)) return
        con.startTran()
        def count = con.executeCommand(command: "UPDATE ${table.fullNameDataset()} SET ${table.sqlObjectName('double')} = ${table.sqlObjectName('double')} + 1", isUpdate: true)
        assertEquals(countRows, count)
        if (!con.autoCommit())
            con.commitTran()
        def q = new QueryDataset(connection: con, query: "SELECT Count(*) AS count_rows FROM ${table.fullNameDataset()} WHERE ${table.sqlObjectName('double')} IS NULL")
        def rows = q.rows()
        assertEquals(1, rows.size())
    }

    protected void bulkLoad(TableDataset bulkTable) {
        if (!con.driver.isOperation(Driver.Operation.BULKLOAD)) return

        def file = bulkTable.csvTempFile
        def count = new Flow().writeTo(dest: file) { updater ->
            (1..countRows).each { num ->
                Map r = GenerationUtils.GenerateRowValues(file.field, false, num)
                r.id1 = num
                r.name = """'name'\t"$num"\ntest|/,;|\\"""

                updater(r)
            }
        }
        assertEquals(countRows, count)

        truncateTable(bulkTable)

        bulkTable.bulkLoadFile(source: file)
        assertEquals(countRows, bulkTable.updateRows)
        assertEquals(countRows, bulkTable.countRow())
    }

    protected void copyWithBulkLoad() {
        if (!con.driver.isOperation(Driver.Operation.BULKLOAD)) return

        def bulkTable = new TableDataset(connection: con)
        bulkTable.tap {
            tableName = 'getl_test_copy_bulk'
            field('id') { type = integerFieldType }
            field('name') { type = stringFieldType; length = 50 }
            field('dt') { type = datetimeFieldType }
            field('num') { type = numericFieldType; length = 12; precision = 2 }
            if (con.driver.isSupport(Driver.Support.BLOB))
                field('bin') { type = blobFieldType; length = 50 }
            create(ifNotExists: true)
        }
        truncateTable(bulkTable)

        def file = TFS.dataset()
        file.field = bulkTable.field
        file.escaped = true
        def countFiles = new Flow().writeTo(dest: file) { updater ->
            updater([:])
            (2..countRows).each { num ->
                Map r = GenerationUtils.GenerateRowValues(file.field, false, num)
                r.id1 = num
                r.name = """'name'\t"$num"\ntest|/,;|\\"""

                updater(r)
            }
        }
        assertEquals(countRows, countFiles)

        def countCopy = new Flow().copy(source: file, dest: bulkTable, bulkLoad: true)
        assertEquals(countRows, countCopy)
        assertEquals(countRows, bulkTable.countRow())
        bulkTable.drop()
    }

    protected void runScript() {
        def table_name = table.fullNameDataset()
        def id1Name = table.sqlObjectName('ID1')
        def id2Name = table.sqlObjectName('ID2')
        def sql = """
----- Test scripter
ECHO Run sql script ...

/*
  test IF operator
*/
ECHO Checking condition ...
IF (1 IN (SELECT $id1Name FROM $table_name)) DO { -- check condition 
    ECHO Table has rows -- test ECHO 
}

/* select to variables */
ECHO Setting variables ...
SET SELECT $id2Name FROM $table_name WHERE $id1Name = 1; -- test SET operator
ECHO For id1=1 then id2={id2}

ECHO Cycle ...
/* FOR CYCLE */FOR (SELECT $id1Name, $id2Name FROM $table_name WHERE $id1Name BETWEEN 2 AND 3) DO { -- test FOR operator
    ECHO For id1={id1} then id2={id2}
}

ECHO Run select ...
/*:select_rows*/
SELECT * FROM $table_name WHERE $id1Name = 1;

ECHO Run update ...
/*:count_update*/
UPDATE $table_name
SET  $id2Name =  $id2Name
WHERE $id1Name = 1;

ECHO {count_update} rows updated
"""
        def scripter = new SQLScripter(connection: table.connection, script: sql)
        scripter.runSql(true)

        scripter.loadFile('resource:/sql/test_scripter.sql')
        scripter.vars.from = (con.currentJDBCDriver.sysDualTable != null)?"FROM ${con.currentJDBCDriver.sysDualTable}":''
        scripter.vars.func = currentTimestampFuncName
        scripter.runSql(true)
        //println MapUtils.ToJson(scripter.vars)
    }

    protected String getCurrentTimestampFuncName() { 'CURRENT_TIMESTAMP()' }

    @Test
    void testOperations() {
        connect()
        assertTrue(con.connected)

        createTable()
        retrieveObject()
        retrieveFields()
        if (insertData() > 0) {
            copyToCsv()
            copyToTable()
            updateData()
            queryData()
            mergeData()
            runCommandUpdate()
        }
        bulkLoad(table)
        copyWithBulkLoad()
        retrieveFields()
        runScript()
        deleteData()
        truncateData()
        deleteRows()
        dropTable()

        disconnect()
        assertFalse(con.connected)
    }

    protected TableDataset createPerfomanceTable(JDBCConnection con, String name, List<Field> fields) {
        def t = new TableDataset(connection: con, schemaName: con.schemaName, tableName: name, field: fields)
        t.drop(ifExists: true)
        t.create()

        return t
    }

    @Test
    void testPerfomance() {
		def c = newCon()
        if (!c.driver.isOperation(Driver.Operation.INSERT)) return

		if (c == null) return
		if (Config.content.perfomanceRows == null) return
		def perfomanceRows = Config.content.perfomanceRows as Integer
		def perfomanceCols = (Config.content.perfomanceCols as Integer)?:100
		Logs.Finest("Test ${c.getClass().name} perfomance write from $perfomanceRows rows with ${perfomanceCols+2} cols ...")

        List<Field> fields = []
        fields << new Field(name: 'id', type: Field.Type.INTEGER, isKey: true)
        fields << new Field(name: 'name', length: 50, isNull: false)
        fields << new Field(name: descriptionName, length: 50, isNull: false)
		(1..perfomanceCols).each { num ->
            fields << new Field(name: "value_$num", type: Field.Type.DOUBLE)
		}

        def t = createPerfomanceTable(c, 'GETL_TEST_PERFOMANCE', fields)
        try {
            def pt = new ProcessTime(name: "${c.driverName} perfomance write")
            new Flow().writeTo(dest: t, dest_batchSize: 500L) { Closure updater ->
                (1..perfomanceRows).each { Integer cur ->
                    cur++
                    def r = [:] as Map<String, Object>
                    r.id = cur
                    r.name = "name $cur"
                    r.put(descriptionName, "description $cur")
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
            t?.drop()
        }
	}

    protected void copyToCsv() {
        def csv = TFS.dataset()
        new Flow().copy(source: table, dest: csv, inheritFields: true)
        assertEquals(table.readRows, csv.writeRows)
    }

    protected void copyToTable() {
        if (!con.driver.isOperation(Driver.Operation.INSERT)) return

        def fp1 = con.currentJDBCDriver.fieldPrefix
        def fp2 = con.currentJDBCDriver.fieldEndPrefix?:fp1
        def table1 = table.cloneDataset() as TableDataset
        table1.readOpts {where = "${fp1}ID1${fp2} > 0" }
        def table2 = table.cloneDataset() as TableDataset
        table2.tap {
            if (con.driver.isSupport(Driver.Support.LOCAL_TEMPORARY)) {
                createOpts {
                    type = localTemporaryTableType
                    onCommit = true
                }
            }
            schemaName = null
            tableName = tableName + '_clone'
            drop(ifExists: true)
            createOpts {indexes.clear() }
            create()
        }

        try {
            def countRows = table1.countRow()
            def fn = table.currentJDBCConnection.currentJDBCDriver.prepareFieldNameForSQL(table.field('value').name)
            table1.copyTo(table2, [value: "CAST(($fn - 1.00) AS decimal(12, 2))"])
            assertEquals(countRows, table2.countRow())
        }
        finally {
            if (table2.type != TableDataset.localTemporaryTableType) {
                table2.connection.connected = false
                table2.connection.connected = true
                table2.drop()
            }
            else {
                table2.drop()
            }
        }
    }

    public String needCatalog

    @Test
    void testRetrieveCatalogs() {
        if (!con.currentJDBCDriver.isSupport(Driver.Support.DATABASE))
            return

        def list = con.retrieveCatalogs()
        println "Detect databases: $list"
        if (needCatalog != null)
            assertTrue(needCatalog in list)
    }

    @Test
    void testRertieveSchemas() {
        if (!con.currentJDBCDriver.isSupport(Driver.Support.SCHEMA))
            return

        def list = con.retrieveSchemas()
        println "Detect schemas: $list"
        if (con.currentJDBCDriver.defaultSchemaName != null)
            assertTrue(con.currentJDBCDriver.defaultSchemaName in list)
    }
}