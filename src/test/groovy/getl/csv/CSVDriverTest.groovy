package getl.csv

import getl.data.Field
import getl.proc.Flow
import getl.tfs.TFS
import getl.utils.DateUtils
import getl.utils.FileUtils
import getl.utils.NumericUtils

import java.sql.Time

/**
 * Created by ascru on 10.11.2016.
 */
class CSVDriverTest extends GroovyTestCase {
    static def fields = [
            new Field(name: 'id', type: 'BIGINT', isKey: true),
            new Field(name: 'name', type: 'STRING', isNull: false, length: 50),
            new Field(name: 'date', type: 'DATE', isNull: false),
            new Field(name: 'time', type: 'TIME', isNull: false, format: 'HH-mm-ss'),
            new Field(name: 'datetime', type: 'DATETIME', isNull: false),
            new Field(name: 'double', type: 'DOUBLE', isNull: false),
            new Field(name: 'numeric', type: 'NUMERIC', isNull: false, length: 12, precision: 2),
            new Field(name: 'boolean', type: 'BOOLEAN', isNull: false),
            new Field(name: 'text', type: 'TEXT', length: 100),
            new Field(name: 'blob', type: 'BLOB', length: 100)
    ]

    static def conParams = [path: "${TFS.systemPath}/test_csv", createPath: true, extension: 'csv', codePage: 'utf-8']

    static {
        FileUtils.ValidPath(conParams.path)
        new File(conParams.path).deleteOnExit()
    }

    void testSchema() {
        def con = new CSVConnection(conParams + [autoSchema: false])
        def csv = new CSVDataset(connection: con, fileName: 'test_schema')
        csv.field = fields
        csv.saveDatasetMetadata()
        assertTrue(new File(csv.fullFileSchemaName()).exists())

        def new_csv = new CSVDataset(connection: con, fileName: 'test_schema')
        new_csv.loadDatasetMetadata()
        assertTrue(new_csv.equalsFields(csv.field))

        assertTrue(FileUtils.DeleteFile(csv.fullFileSchemaName()))
    }

    private void validReadWrite(CSVConnection con, String name) {
        con.autoSchema = true
        def csv = new CSVDataset(connection: con, fileName: name, formatDateTime: DateUtils.defaultDateTimeMask)
        csv.field = fields

        def generate_row = { id ->
            def row = [:]

            row.id = id
            row.name = "row $id"
            row.date = DateUtils.ParseDate('2016-10-15')
            row.time = new java.sql.Time(9, 15, 30)
            row.datetime = DateUtils.ParseDateTime('2016-10-16 09:15:30.123')
            row.double = 123456789.12
            row.numeric = new BigDecimal(123456789.12)
            row.boolean = true
            row.text = "text \"$id\"\tand\nnew line"
            row.blob = 'abcdef'.bytes

            return row
        }

        new Flow().writeTo(dest: csv, dest_splitSize: 10000) { updater ->
            (1..100).each { id ->
                updater(generate_row(id))
            }
        }
        assertEquals(2, csv.countWritePortions())
        assertEquals(100, csv.writeRows)

        con.autoSchema = true
        def new_csv = new CSVDataset(connection: con, fileName: name, formatDateTime: DateUtils.defaultDateTimeMask, autoSchema: true)
        def id = 0
        new_csv.eachRow(isSplit: true) { row ->
            id++

            assertTrue(row.id instanceof Long)
            assertTrue(row.date instanceof Date)
            assertTrue(row.time instanceof Date)
            assertTrue(row.datetime instanceof Date)
            assertTrue(row.double instanceof Double)
            assertTrue(row.numeric instanceof BigDecimal)
            assertTrue(row.boolean instanceof Boolean)
            assertTrue(row.text instanceof String)
            assertTrue(row.blob instanceof byte[])

            assertEquals(id, row.id)
            assertEquals("row $id", row.name)
            assertEquals(DateUtils.ParseDate('2016-10-15'), row.date)
            assertEquals(new java.sql.Time(9, 15, 30), row.time)
            assertEquals(DateUtils.ParseDateTime('2016-10-16 09:15:30.123'), row.datetime)
            assertEquals(123456789.12, row.double)
            assertEquals(NumericUtils.Round(new BigDecimal(123456789.12), 2), row.numeric)
            assertTrue(row.boolean)
            assertEquals("text \"$id\"\tand\nnew line", row.text)
            assertEquals('abcdef'.bytes, row.blob)
        }
        assertEquals(2, new_csv.countReadPortions())
        assertEquals(100, new_csv.readRows)

        def unwrite_csv = new CSVDataset(connection: con, fileName: "${name}_unwrite", formatDateTime: DateUtils.defaultDateTimeMask, schemaFileName: csv.fullFileSchemaName(), deleteOnEmpty: true)
        unwrite_csv.loadDatasetMetadata()
        unwrite_csv.openWrite(isValid: true)
        def row = generate_row(1)
        row.name = null
        unwrite_csv.write(row)
        shouldFail { unwrite_csv.doneWrite() }
        unwrite_csv.closeWrite()

        def bad_csv = new CSVDataset(connection: con, fileName: "${name}_bad", decimalSeparator: ',', escapeProcessLineChar: '[enter]')
        bad_csv.field = fields
        bad_csv.openWrite()

        row = generate_row(1)
        bad_csv.write(row)

        row = generate_row(2)
        row.id = 1
        bad_csv.write(row)

        row = generate_row(3)
        row.name = null
        bad_csv.write(row)

        bad_csv.doneWrite()
        bad_csv.closeWrite()

        bad_csv.fieldByName('datetime').with {
            type = Field.Type.STRING
            length = 25
        }

        bad_csv.openWrite(append: true)

        row = generate_row(4)
        row.datetime = 'invalid date'
        bad_csv.write(row)

        bad_csv.doneWrite()
        bad_csv.closeWrite()

        bad_csv.field = fields
        def errors = [:]
        def regError = { Exception error, Long line ->
            errors.put(line.toString(), error.message)
            return true
        }
        def correct_rows = bad_csv.rows (processError: regError, isValid: true)
        assertEquals(1, correct_rows.size())
        assertTrue((errors.get('3') as String).indexOf('duplicate value') != -1)
        assertTrue((errors.get('4') as String).indexOf('null value encountered') != -1)
        assertTrue((errors.get('5') as String).indexOf('invalid date') != -1)

        con.autoSchema = true
        csv.drop(portions: csv.countWritePortions())

        shouldFail() { unwrite_csv.drop(validExist: true) }
        bad_csv.drop()
    }

    void testWindowsCSV() {
        def con = new CSVConnection(conParams + [escaped: false, header: true, isGzFile: false])
        validReadWrite(con, 'windows')

        con = new CSVConnection(conParams + [escaped: false, header: false, isGzFile: true])
        validReadWrite(con, 'windows-gz')

        con = new CSVConnection(conParams + [escaped: true, header: true, isGzFile: false])
        validReadWrite(con, 'unix')


    }
}
