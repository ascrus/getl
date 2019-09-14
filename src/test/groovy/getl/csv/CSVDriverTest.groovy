package getl.csv

import getl.data.Dataset
import getl.data.Field
import getl.proc.Flow
import getl.stat.ProcessTime
import getl.tfs.TFS
import getl.utils.DateUtils
import getl.utils.FileUtils
import getl.utils.Logs
import getl.utils.NumericUtils
import org.junit.BeforeClass
import org.junit.Test

/**
 * Created by ascru on 10.11.2016.
 */
class CSVDriverTest extends getl.test.GetlTest {
    static def fields = [
            new Field(name: 'ID', type: 'BIGINT', isKey: true),
            new Field(name: 'Name', type: 'STRING', isNull: false, length: 50),
            new Field(name: 'Date', type: 'DATE', isNull: false),
            new Field(name: 'Time', type: 'TIME', isNull: false),
            new Field(name: 'DateTime', type: 'DATETIME', isNull: false),
            new Field(name: 'Double', type: 'DOUBLE', isNull: false),
            new Field(name: 'Numeric', type: 'NUMERIC', isNull: false, length: 12, precision: 2),
            new Field(name: 'Boolean', type: 'BOOLEAN', isNull: false),
            new Field(name: 'Text', type: 'TEXT', length: 100),
            new Field(name: 'Blob', type: 'BLOB', length: 100)
    ]

    static Map<String, Object> conParams = [path: "${TFS.systemPath}/test_csv", createPath: true, extension: 'csv',
                                            codePage: 'utf-8']

    @BeforeClass
    static void InitTest() {
        FileUtils.ValidPath(conParams.path)
        new File(conParams.path).deleteOnExit()
    }

    @Test
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

        def resCsv = new CSVDataset(connection: con, schemaFileName: 'resource:/csv/csv.schema')
        assertTrue(resCsv.isResourceFileNameSchema())
        resCsv.loadDatasetMetadata()
        assertEquals(2, resCsv.field.size())
        assertNotNull(resCsv.field('id'))
        assertNotNull(resCsv.field('name'))
    }

    private void validReadWrite(CSVConnection con, String name) {
        con.autoSchema = true
        def csv = new CSVDataset(connection: con, fileName: name)
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

        new Flow().writeTo(dest: csv, dest_append: true) { updater ->
            (1..100).each { id ->
                updater(generate_row(id))
            }
        }
        assertEquals(100, csv.writeRows)
        new Flow().writeTo(dest: csv, dest_append: true) { updater ->
            (101..200).each { id ->
                updater(generate_row(id))
            }
        }
        assertEquals(100, csv.writeRows)

        def csvCountRows = 0
        csv.eachRow { Map row ->
			assertNotNull(row.id)
			assertNotNull(row.name)
			assertNotNull(row.date)
			assertNotNull(row.time)
			assertNotNull(row.datetime)
			assertNotNull(row.double)
			assertNotNull(row.numeric)
			assertNotNull(row.boolean)
			assertNotNull(row.text)
			assertNotNull(row.blob)

			csvCountRows++
		}
        assertEquals(200, csv.readRows)
        assertEquals(200, csvCountRows)

		def csv_without_fields = new CSVDataset(connection: con, fileName: name)
		csvCountRows = 0
		csv_without_fields.eachRow { Map row ->
			assertNotNull(row.id)
			assertNotNull(row.name)
			assertNotNull(row.date)
			assertNotNull(row.time)
			assertNotNull(row.datetime)
			assertNotNull(row.double)
			assertNotNull(row.numeric)
			assertNotNull(row.boolean)
			assertNotNull(row.text)
			assertNotNull(row.blob)

			csvCountRows++
		}
		assertEquals(200, csv_without_fields.readRows)
		assertEquals(200, csvCountRows)
		def origFieldNameList = Dataset.Fields2List(fields)
		def newFieldNameList = Dataset.Fields2List(csv_without_fields.field)
		assertEquals(newFieldNameList, origFieldNameList)

        def csvFileName = csv.fullFileName()
		new Flow().writeTo(dest: csv, dest_splitSize: 10000) { updater ->
            (1..100).each { id ->
                updater(generate_row(id))
            }
        }
        assertEquals(2, csv.countWritePortions)
        assertEquals(100, csv.writeRows)
        FileUtils.DeleteFile(csvFileName)

        con.autoSchema = true
        def new_csv = new CSVDataset(connection: con, fileName: name, autoSchema: true)
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
        assertEquals(2, new_csv.countReadPortions)
        assertEquals(100, new_csv.readRows)

        def unwrite_csv = new CSVDataset(connection: con, fileName: "${name}_unwrite",
                schemaFileName: csv.fullFileSchemaName(), deleteOnEmpty: true)
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
        csv.drop(portions: csv.countWritePortions)

        shouldFail() { unwrite_csv.drop(validExist: true) }
        bad_csv.drop()
    }

    @Test
    void testWindowsCSV() {
        def con = new CSVConnection(conParams +
                [escaped: false, header: true, isGzFile: false, locale: 'ru-RU', decimalSeparator: ',',
                 fieldDelimiter: ';', formatDate: 'dd MMM yyyy',
                 formatTime: 'HH:mm:ss.SSS', formatDateTime: 'dd MMM yyyy HH:mm:ss.SSS'])
        validReadWrite(con, 'windows')

        con = new CSVConnection(conParams +
                [escaped: false, header: false, isGzFile: true, formatDateTime: 'yyyy-MM-dd HH:mm:ss.SSS'])
        validReadWrite(con, 'windows-gz')

        con = new CSVConnection(conParams +
                [escaped: true, header: true, isGzFile: false, formatDateTime: 'yyyy-MM-dd HH:mm:ss.SSS'])
        validReadWrite(con, 'unix')
    }

    @Test
    void testRowDelimiter() {
        def con = new CSVConnection(conParams + [header: false, rowDelimiter: '\n'])
        def ds1 = new CSVDataset(connection: con, fileName: 'test_row_delimiter_1')
        ds1.field << new Field(name: 'Id', type: Field.Type.INTEGER, isKey: true)
        ds1.field << new Field(name: 'Name', length: 50, isNull: false)
        new Flow().writeTo(dest: ds1) { updater ->
            (1..3).each { num ->
                Map row = [id: num, name: "name $num"]
                updater(row)
            }
        }

        def text1 = new File(ds1.fullFileName()).text
        assertEquals('1,name 1\n2,name 2\n3,name 3\n', text1)

        def ds2 = new CSVDataset(connection: con, fileName: 'test_row_delimiter_2', rowDelimiter: '\r\n')
        ds2.field = ds1.field
        new Flow().copy(source: ds1, dest: ds2)

        def text2 = new File(ds2.fullFileName()).text
        assertEquals('1,name 1\r\n2,name 2\r\n3,name 3\r\n', text2)

        ds1.drop()
        ds2.drop()
    }

    @Test
    void testPerfomance() {
		def perfomanceRows = 1000
		def perfomanceCols = 1000

		Logs.Finest("Test CSV perfomance write from $perfomanceRows rows with ${perfomanceCols+2} cols ...")

		def c = new CSVConnection(conParams + (Map<String, Object>)([autoSchema: (Object)false]))
		def t = new CSVDataset(connection: c, fileName: 'test_perfomance')

		t.field << new Field(name: 'Id', type: Field.Type.INTEGER, isKey: true)
		t.field << new Field(name: 'Name', length: 50, isNull: false)
		(1..perfomanceCols).each { num ->
			t.field << new Field(name: "Value_$num", type: Field.Type.DOUBLE)
		}

		try {
			def pt = new ProcessTime(name: "CSV perfomance write")
			new Flow().writeTo(dest: t, dest_batchSize: 1000) { Closure updater ->
				(1..perfomanceRows).each { Integer cur ->
					def r = [:] as Map<String, Object>
					r.id = cur
					r.name = "name $cur"
					(1..perfomanceCols).each { Integer num ->
						r.put("value_$num".toString(), cur)
					}
					updater(r)
				}
			}
			pt.finish(perfomanceRows as Long)

			pt = new ProcessTime(name: "CSV perfomance read")
			def cur = 0
			new Flow().process(source: t) { Map<String, Object> r ->
				cur++
				assertEquals(cur, r.id)
				assertEquals("name $cur", r.name)
			}
			pt.finish(cur as Long)
			assertEquals(perfomanceRows, cur)

			def n = TFS.dataset()
			new Flow().copy(source: t, dest: n, inheritFields: true, excludeFields: t.field*.name - ['Id', 'Name'])
			assertEquals(2, n.field.size())
			cur  = 0
			n.eachRow { Map<String, Object> r ->
				cur++
				assertEquals(cur, r.id)
				assertEquals("name $cur", r.name)
			}
			assertEquals(perfomanceRows, cur)
		}
		finally {
			t.drop()
		}
	}
}
