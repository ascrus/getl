package getl.jdbc

import getl.data.*
import getl.driver.Driver
import getl.proc.Flow
import getl.tfs.TDS
import getl.tfs.TFS
import getl.utils.DateUtils
import getl.utils.GenerationUtils
import groovy.transform.InheritConstructors

/**
 * Created by ascru on 21.11.2016.
 */
@InheritConstructors
abstract class JDBCDriverProto extends GroovyTestCase {
    private JDBCConnection _con
    abstract protected JDBCConnection newCon()
    public JDBCConnection getCon() {
        if (_con == null) _con = newCon()
        return _con
    }
    final def table = new TableDataset(connection: con, tableName: '_getl_test')
    List<Field> getFields () {
        def res =
            [
                new Field(name: 'id1', type: 'BIGINT', isKey: true, ordKey: 1),
                new Field(name: 'id2', type: 'DATETIME', isKey: true, ordKey: 2),
                new Field(name: 'name', type: 'STRING', length: 50, isNull: false),
                new Field(name: 'value', type: 'NUMERIC', length: 12, precision: 2),
                new Field(name: 'double', type: 'DOUBLE'),
                new Field(name: 'date', type: 'DATE', isNull: false),
                new Field(name: 'time', type: 'TIME'),
                new Field(name: 'flag', type: 'BOOLEAN', isNull: false, defaultValue: true)
            ]

        if (con != null && con.driver.isSupport(Driver.Support.BLOB)) res << new Field(name: 'data', type: 'BLOB', length: 1024)
        if (con != null && con.driver.isSupport(Driver.Support.CLOB)) new Field(name: 'text', type: 'TEXT', length: 1024)

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
            table.create(indexes: [
                    _getl_test_idx_1: [columns: ['id1', 'date'], unique: true],
                    _getl_test_idx_2: [columns: ['id2', 'name']]
            ])
        }
        else {
            table.create()
        }
        assertTrue(table.exists)
    }

    public void testLocalTable() {
        if (!con.driver.isSupport(Driver.Support.TEMPORARY)) {
            println "Skip test temporary table: ${con.driver.getClass().name} not support this futures"
            return
        }
        def tempTable = new TableDataset(connection: con, tableName: '_getl_local_temp_test', type: JDBCDataset.Type.LOCAL_TEMPORARY)
        tempTable.field = fields
        if (con.driver.isSupport(Driver.Support.INDEX)) {
            tempTable.create(indexes: [
                    _getl_local_temp_test_idx_1: [columns: ['id1', 'date'], unique: true],
                    _getl_local_temp_test_idx_2: [columns: ['id2', 'name']]
            ])
        }
        else {
            tempTable.create()
        }
        tempTable.drop(ifExists: true)
    }

    public void testGlobalTable() {
        if (!con.driver.isSupport(Driver.Support.TEMPORARY)) {
            println "Skip test temporary table: ${con.driver.getClass().name} not support this futures"
            return
        }
        def tempTable = new TableDataset(connection: con, tableName: '_getl_global_temp_test', type: JDBCDataset.Type.GLOBAL_TEMPORARY)
        tempTable.field = fields
        assertFalse(tempTable.exists)
        if (con.driver.isSupport(Driver.Support.INDEX)) {
            tempTable.create(indexes: [
                    _getl_global_temp_test_idx_1: [columns: ['id1', 'date'], unique: true],
                    _getl_global_temp_test_idx_2: [columns: ['id2', 'name']]
            ])
        }
        else {
            tempTable.create()
        }
        assertTrue(tempTable.exists)
        tempTable.drop(ifExists: true)
    }

    private void dropTable() {
        table.drop()
        assertFalse(table.exists)
    }

    private void retrieveFields() {
        table.field.clear()
        table.retrieveFields()
        assertEquals(fields, table.field)
    }

    private void insertData() {
        def count = new Flow().writeTo(dest: table) { updater ->
            (1..1000).each { num ->
                def r = [:]
                r.id1 = num
                r.id2 = new Date()
                r.name = "String $num"
                r.value = GenerationUtils.GenerateNumeric(12, 2)
                r.double = GenerationUtils.GenerateDouble()
                r.date = GenerationUtils.GenerateDate()
                r.time = new java.sql.Time(GenerationUtils.GenerateInt(0, 360000))
                r.flag = GenerationUtils.GenerateBoolean()
                r.text = GenerationUtils.GenerateString(1024)
                r.data = GenerationUtils.GenerateString(512).bytes

                updater(r)
            }
        }
        assertEquals(1000, count)
    }

    private void updateData() {
        def rows = table.rows()
        def count = new Flow().writeTo(dest: table, dest_operation: 'UPDATE') { updater ->
            rows.each { r ->
                r.name += ' update'
                r.value += 1
                r.double += 1
                r.date = DateUtils.AddDate('ss', 1, r.date)
                r.time += 100
                r.flag = GenerationUtils.GenerateBoolean()
                r.text = GenerationUtils.GenerateString(1024)
                r.data = GenerationUtils.GenerateString(512).bytes

                updater(r)
            }
        }
        assertEquals(1000, count)
    }

    private void mergeData() {
        def rows = table.rows()
        def count = new Flow().writeTo(dest: table, dest_operation: 'MERGE') { updater ->
            rows.each { r ->
                r.name += ' merge'
                r.value += 1
                r.double += 1
                r.date = DateUtils.AddDate('ss', 1, r.date)
                r.time += 100
                r.flag = GenerationUtils.GenerateBoolean()
                r.text = GenerationUtils.GenerateString(1024)
                r.data = GenerationUtils.GenerateString(512).bytes

                updater(r)
            }
        }
        assertEquals(1000, count)
    }

    private void validCount() {
        def q = new QueryDataset(connection: con, query: "SELECT Count(*) AS count_rows FROM ${table.fullNameDataset()} WHERE double IS NOT NULL")
        def rows = q.rows()
        assertEquals(1, rows.size())
        assertEquals(1000, rows[0].count_rows)
    }

    private void deleteData() {
        def rows = table.rows(onlyFields: ['ID1', 'ID2'])
        def count = new Flow().writeTo(dest: table, dest_operation: 'DELETE') { updater ->
            rows.each { r ->
                updater(r)
            }
        }
        assertEquals(1000, count)
    }

    private void truncateData() {
        table.truncate(truncate: true)
    }

    private void validCountZero() {
        def q = new QueryDataset(connection: con, query: "SELECT Count(*) AS count_rows FROM ${table.fullNameDataset()}")
        def rows = q.rows()
        assertEquals(1, rows.size())
        assertEquals(0, rows[0].count_rows)
    }

    private void runCommandUpdate() {
        con.startTran()
        def count = con.executeCommand(command: "UPDATE ${table.fullNameDataset()} SET double = NULL", isUpdate: true)
        assertEquals(1000, count)
        con.commitTran()
        def q = new QueryDataset(connection: con, query: "SELECT Count(*) AS count_rows FROM ${table.fullNameDataset()} WHERE double IS NULL")
        def rows = q.rows()
        assertEquals(1, rows.size())
        assertEquals(1000, rows[0].count_rows)
    }

    private void bulkLoad() {
        def file = TFS.dataset()
        def count = new Flow().copy(source: table, dest: file, inheritFields: true)
        assertEquals(1000, count)
        truncateData()
        validCountZero()
        table.bulkLoadFile(source: file)
        validCount()
    }

    void testOperations() {
        connect()
        createTable()
        retrieveFields()
        insertData()
        validCount()
        updateData()
        validCount()
        if (con.driver.isOperation(Driver.Operation.MERGE)) {
            mergeData()
            validCount()
        }
        if (con.driver.isOperation(Driver.Operation.BULKLOAD)) {
            bulkLoad()
        }
        runCommandUpdate()
        deleteData()
        validCountZero()
        dropTable()
        disconnect()
    }
}
