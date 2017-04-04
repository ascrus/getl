package getl.jdbc

import getl.data.*
import getl.driver.Driver
import getl.proc.Flow
import getl.tfs.TFS
import getl.utils.DateUtils
import getl.utils.GenerationUtils
import getl.utils.NumericUtils
import getl.utils.StringUtils
import groovy.transform.InheritConstructors

import java.sql.Time

/**
 * Created by ascru on 21.11.2016.
 */
@InheritConstructors
abstract class JDBCDriverProto extends GroovyTestCase {
    private static final countRows = 100
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
                new Field(name: 'value', type: 'NUMERIC', length: 12, precision: 2, isNull: false),
                new Field(name: 'double', type: 'DOUBLE', isNull: false),
                new Field(name: 'date', type: 'DATE', isNull: false),
                new Field(name: 'time', type: 'TIME', isNull: false),
                new Field(name: 'flag', type: 'BOOLEAN', isNull: false, defaultValue: true)
            ]

        if (con != null && con.driver.isSupport(Driver.Support.BLOB)) res << new Field(name: 'data', type: 'BLOB', length: 1024)
        if (con != null && con.driver.isSupport(Driver.Support.CLOB)) res << new Field(name: 'text', type: 'TEXT', length: 1024)

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
        if (!con.driver.isSupport(Driver.Support.LOCAL_TEMPORARY)) {
            println "Skip test local temporary table: ${con.driver.getClass().name} not support this futures"
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
        if (!con.driver.isSupport(Driver.Support.GLOBAL_TEMPORARY)) {
            println "Skip test global temporary table: ${con.driver.getClass().name} not support this futures"
            return
        }
        def tempTable = new TableDataset(connection: con, tableName: '_getl_global_temp_test', type: JDBCDataset.Type.GLOBAL_TEMPORARY)
        tempTable.field = fields
        tempTable.drop(ifExists: true)
        if (con.driver.isSupport(Driver.Support.INDEX)) {
            tempTable.create(indexes: [
                    _getl_global_temp_test_idx_1: [columns: ['id1', 'date'], unique: true],
                    _getl_global_temp_test_idx_2: [columns: ['id2', 'name']]
            ])
        }
        else {
            tempTable.create()
        }
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
            (1..countRows).each { num ->
                def r = [:]
                r = GenerationUtils.GenerateRowValues(table.field, num)
                r.id1 = num

                updater(r)
            }
        }
        assertEquals(countRows, count)
        validCount()
    }

    private void updateData() {
        def rows = table.rows(order: ['id1'])
        def count = new Flow().writeTo(dest: table, dest_operation: 'UPDATE') { updater ->
            rows.each { r ->
                Map nr = [:]
                nr.putAll(r)
                nr.name = StringUtils.LeftStr(r.name, 40) + ' update'
                nr.value = r.value + 1
                nr.double = r.double + 1.00
                nr.date = DateUtils.AddDate('dd', 1, r.date)
                nr.time = java.sql.Time.valueOf((r.time as java.sql.Time).toLocalTime().plusSeconds(100))
                nr.flag = GenerationUtils.GenerateBoolean()
                nr.text = GenerationUtils.GenerateString(1024)
                nr.data = GenerationUtils.GenerateString(512).bytes

                updater(nr)
            }
        }
        assertEquals(countRows, count)
        validCount()

        def i = 0
        table.eachRow(order: ['id1']) { r ->
            assertEquals(StringUtils.LeftStr(rows[i].name, 40) + ' update', r.name)
            assertEquals(rows[i].value + 1, r.value)
            assertEquals(rows[i].double + 1.00, r.double)
            assertEquals(DateUtils.AddDate('dd', 1, rows[i].date), r.date)
            assertEquals(java.sql.Time.valueOf((rows[i].time as java.sql.Time).toLocalTime().plusSeconds(100)), r.time)
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
                nr.value = r.value + 1
                nr.double = r.double + 1.00
                nr.date = DateUtils.AddDate('dd', 1, r.date)
                nr.time = java.sql.Time.valueOf((r.time as java.sql.Time).toLocalTime().plusSeconds(100))
                nr.flag = GenerationUtils.GenerateBoolean()
                nr.text = GenerationUtils.GenerateString(1024)
                nr.data = GenerationUtils.GenerateString(512).bytes

                updater(nr)
            }
        }
        assertEquals(countRows, count)
        validCount()

        def i = 0
        table.eachRow(order: ['id1']) { r ->
            assertEquals(StringUtils.LeftStr(rows[i].name, 40) + ' merge', r.name)
            assertEquals(rows[i].value + 1, r.value)
            assertEquals(rows[i].double + 1.00, r.double)
            assertEquals(DateUtils.AddDate('dd', 1, rows[i].date), r.date)
            assertEquals(java.sql.Time.valueOf((rows[i].time as java.sql.Time).toLocalTime().plusSeconds(100)), r.time)
            i++
        }
    }

    private void validCount() {
        def q = new QueryDataset(connection: con, query: "SELECT Count(*) AS count_rows FROM ${table.fullNameDataset()} WHERE id1 IS NOT NULL")
        def rows = q.rows()
        assertEquals(1, rows.size())
        assertEquals(countRows, rows[0].count_rows)
    }

    private void deleteData() {
        def rows = table.rows(onlyFields: ['ID1', 'ID2'])
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

    private void validCountZero() {
        def q = new QueryDataset(connection: con, query: "SELECT Count(*) AS count_rows FROM ${table.fullNameDataset()}")
        def rows = q.rows()
        assertEquals(1, rows.size())
        assertEquals(0, rows[0].count_rows)
    }

    private void runCommandUpdate() {
        con.startTran()
        def count = con.executeCommand(command: "UPDATE ${table.fullNameDataset()} SET double = double + 1", isUpdate: true)
        assertEquals(countRows, count)
        con.commitTran()
        def q = new QueryDataset(connection: con, query: "SELECT Count(*) AS count_rows FROM ${table.fullNameDataset()} WHERE double IS NULL")
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

    void testOperations() {
        connect()
        createTable()
        retrieveFields()
        insertData()
        updateData()
        if (con.driver.isOperation(Driver.Operation.MERGE)) {
            mergeData()
        }
        if (con.driver.isOperation(Driver.Operation.BULKLOAD)) {
            bulkLoad()
        }
        runCommandUpdate()
        deleteData()
        dropTable()
        disconnect()
    }
}
