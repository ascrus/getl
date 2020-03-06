package getl.csv

import getl.data.Dataset
import getl.data.Field
import getl.lang.Getl
import getl.proc.Flow
import getl.stat.ProcessTime
import getl.tfs.TFS
import getl.utils.DateUtils
import getl.utils.FileUtils
import getl.utils.Logs
import getl.utils.NumericUtils
import getl.utils.StringUtils
import groovy.transform.CompileStatic
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

        if (!csv.isGzFile) {
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
        }
        else {
            new Flow().writeTo(dest: csv, dest_append: false) { updater ->
                (1..200).each { id ->
                    updater(generate_row(id))
                }
            }
        }

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

        def bad_csv = new CSVDataset(connection: con, fileName: "${name}_bad", decimalSeparator: ',')
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
    void testEscapeCsv() {
        def con = new CSVConnection(conParams +
                [escaped: true, header: true, isGzFile: false, nullAsValue: '<NULL>', constraintsCheck: true])
        def ds = new CSVDataset(connection: con, fileName: 'test.escape')
        ds.field << new Field(name: 'id', type: Field.Type.INTEGER, isKey: true)
        ds.field << new Field(name: 'name', isNull: false)
        ds.field << new Field(name: 'value', type: Field.Type.INTEGER)
        ds.field << new Field(name: 'text', type: Field.Type.TEXT)
        new Flow().writeTo(dest: ds) { updater ->
            def r = [id: 1, name: '123"456\'789"', text: '123"456\'789,\nabc']
            updater(r)
        }
        def file = new File(ds.fullFileName())
        try {
            def text = '''id,name,value,text
1,"123\\"456\\'789\\"",<NULL>,"123\\"456\\'789,\\nabc"
'''
            assertEquals(text, file.text)
            //        println '>>>\n' + file.text + '\n>>>'

            ds.eachRow { r ->
                assertEquals(1, r.id)
                assertEquals('123"456\'789"', r.name)
                assertNull(r.value)
                assertEquals('123"456\'789,\nabc', r.text)
            }
        }
        finally {
            file.delete()
        }
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
    }

    @Test
    void testLinuxCSV() {
        def con = new CSVConnection(conParams +
                [escaped: true, header: true, isGzFile: false, formatDateTime: 'yyyy-MM-dd HH:mm:ss.SSS'])
        validReadWrite(con, 'unix')

        con = new CSVConnection(conParams +
                [escaped: true, header: true, isGzFile: true, formatDateTime: 'yyyy-MM-dd HH:mm:ss.SSS'])
        validReadWrite(con, 'unix-gz')
    }

    @Test
    void testRowDelimiter() {
        def con = new CSVConnection(conParams + [header: false, rowDelimiter: '\n'])
        def ds1 = new CSVDataset(connection: con, fileName: 'test_row_delimiter_1')
        ds1.field << new Field(name: 'Id', type: Field.integerFieldType, isKey: true)
        ds1.field << new Field(name: 'Name', length: 50, isNull: false)
        ds1.field << new Field(name: 'Result Time', type: Field.dateFieldType, isNull: false)
        new Flow().writeTo(dest: ds1) { updater ->
            (1..3).each { num ->
                Map row = [id: num, name: "name $num", 'result time': DateUtils.ParseDate('2020-01-01')]
                updater(row)
            }
        }

        def text1 = new File(ds1.fullFileName()).text
        assertEquals('1,name 1,2020-01-01\n2,name 2,2020-01-01\n3,name 3,2020-01-01\n', text1)

        def ds2 = new CSVDataset(connection: con, fileName: 'test_row_delimiter_2', rowDelimiter: '\r\n')
        ds2.field = ds1.field
        ds2.field('Result Time').name = 'result_time'
        new Flow().copy(source: ds1, dest: ds2) { i, o ->
            o.'result_time' = i.'result time'
        }

        def text2 = new File(ds2.fullFileName()).text
        assertEquals('1,name 1,2020-01-01\r\n2,name 2,2020-01-01\r\n3,name 3,2020-01-01\r\n', text2)

        ds1.drop()
        ds2.drop()
    }

    static def perfomanceStringValue = StringUtils.Replicate('0', 23) + '"' + '\n' + "'" + StringUtils.Replicate('0', 23)

    @Test
    void testPerfomanceWindows() {
        doPerfomance('Windows format with constraints',
                conParams + [escaped: false, codePage: 'cp1251', constraintsCheck: true, nullAsValue: '<NULL>'])
    }

    @Test
    void testPerfomanceLinux() {
        doPerfomance('Linux format with constraints',
                conParams + [escaped: true, codePage: 'utf-8', constraintsCheck: true, nullAsValue: '<NULL>'])
    }

    @CompileStatic
    private void doPerfomance(String testName, Map testParams) {
		def perfomanceRows = 50000
		def perfomanceCols = 50

		Logs.Finest("Test perfomance for $testName ($perfomanceRows rows, ${perfomanceCols+2} cols) ...")

		def c = new CSVConnection(testParams + (Map<String, Object>)([autoSchema: (Object)false]))
		def t = new CSVDataset(connection: c, fileName: 'test_perfomance')

		t.field << new Field(name: 'Id', type: Field.Type.INTEGER, isKey: true)
		t.field << new Field(name: 'Name', length: 50, isNull: false)
		(1..perfomanceCols).each { num ->
            def f = new Field(name: "Value_$num", type: Field.Type.STRING, length: 50, isNull: false)
            if (num.mod(10) == 0) f.isNull = true
			t.field << f
		}

		try {
			def pt = new ProcessTime(name: "CSV perfomance write")
			new Flow().writeTo(dest: t) { Closure updater ->
				(1..perfomanceRows).each { Integer cur ->
					def r = [:] as Map<String, Object>
					r.id = cur
					r.name = perfomanceStringValue
					(1..perfomanceCols).each { Integer num ->
                        if (num.mod(10) == 0)
                            r.put("value_$num".toString(), null as String)
                        else
						    r.put("value_$num".toString(), perfomanceStringValue)
					}
					updater(r)
				}
			}
            def size = new File(t.fullFileName()).size()
            pt.name = "CSV perfomance write (${FileUtils.SizeBytes(size)})"
			pt.finish(perfomanceRows as Long)


			pt = new ProcessTime(name: "CSV perfomance read (${FileUtils.SizeBytes(size)})")
			def cur = 0
			new Flow().process(source: t) { Map<String, Object> r ->
				cur++
				assertEquals(cur, r.id)
                assertEquals(perfomanceStringValue, r.name)
			}
			pt.finish(cur as Long)
			assertEquals(perfomanceRows, cur)
		}
		finally {
			t.drop()
		}
	}

    @Test
    void testAvaibleSplit() {
        Getl.Dsl(this) {
            def csv = csv {
                useConnection csvConnection { path = csvTempConnection().path }
                fileName = 'test.split'
                field('id') { type = integerFieldType}
                field('name')
                writeOpts {
                    batchSize = 0
                    avaibleAfterWrite = true
                    splitFile { true }
                    deleteOnEmpty = true
                }
            }

            rowsTo(csv) {
                writeRow { add ->
                    (1..3).each { num ->
                        add id: num, name: "name $num"
                        assertTrue(new File(csv.fullFileName(num)).exists())
                    }
                }
            }

            assertEquals(3, csv.writedFiles.size())
            csv.writedFiles.each {
                assertTrue(new File(it.fileName).exists())
            }

            csv.drop(portions: 3)
            csv.writedFiles.each {
                assertFalse(new File(it.fileName).exists())
            }
        }
    }

    @Test
    void testNullValue() {
        Getl.Dsl(this) {
            csvTemp { ds ->
                writeOpts {
                    isValid = true
                }

                header = true

                field('id') { type = integerFieldType; isKey = true }
                field('name') { isNull = false }
                field('v1') { type = integerFieldType }
                field('v2') { type = stringFieldType }
                field('v3') { type = dateFieldType }
                field('v4') { type = numericFieldType; length = 12; precision = 2 }
                field('v5') { type = booleanFieldType }

                def write = {
                    rowsTo(ds) {
                        writeRow { add ->
                            add id: 1, name: 'one', v1: 1, v2: '"string"', v3: DateUtils.ParseDate('2019-12-31'), v4: 123.45, v5: true
                            add id: 2, name: 'two'
                        }
                    }
                }

                def read = {
                    rowsProcess(ds) {
                        int i = 0
                        readRow { row ->
                            i++

                            if (i == 1) {
                                assertEquals(1, row.id)
                                assertEquals('one', row.name)
                                assertEquals(1, row.v1)
                                assertEquals('"string"', row.v2)
                                assertEquals(DateUtils.ParseDate('2019-12-31'), row.v3)
                                assertEquals(123.45, row.v4)
                                assertEquals(true, row.v5)
                            }
                            else {
                                assertEquals(2, row.id)
                                assertEquals('two', row.name)
                                assertNull(row.v1)
                                assertNull(row.v2)
                                assertNull(row.v3)
                                assertNull(row.v4)
                                assertNull(row.v5)
                            }
                        }
                    }

                    return new File(ds.fullFileName()).text
                }

                escaped = true
                nullAsValue = '\u0000'
                write()
                assertEquals('''id|name|v1|v2|v3|v4|v5
1|"one"|1|"\\"string\\""|2019-12-31|123.45|1
2|"two"|\u0000|\u0000|\u0000|\u0000|\u0000
''',read())

                escaped = false
                nullAsValue = '\u0000'
                write()
                assertEquals('''id|name|v1|v2|v3|v4|v5
1|one|1|"""string"""|2019-12-31|123.45|1
2|two|\u0000|\u0000|\u0000|\u0000|\u0000
''',read())

                escaped = true
                nullAsValue = '\\\\'
                write()
                assertEquals('''id|name|v1|v2|v3|v4|v5
1|"one"|1|"\\"string\\""|2019-12-31|123.45|1
2|"two"|\\\\|\\\\|\\\\|\\\\|\\\\
''',read())

                escaped = false
                nullAsValue = '\\\\'
                write()
                assertEquals('''id|name|v1|v2|v3|v4|v5
1|one|1|"""string"""|2019-12-31|123.45|1
2|two|\\\\|\\\\|\\\\|\\\\|\\\\
''',read())
            }
        }
    }

    @Test
    void testPresetMode() {
        Getl.Dsl(this) {
            CSVConnection.PresetModes.each { mode, modeParams ->
                def con = csvConnection {
                    presetMode = mode
                }
                modeParams.each { k, v ->
                    assertEquals(v, con.params.get(k))
                }

                def file = csv {
                    presetMode = mode
                }
                modeParams.each { k, v ->
                    assertEquals(v, file.params.get(k))
                }
            }

            def configName = textFile {
                temporaryFile = true
                write {
                    connections {
                        traditionalPresetMode {
                            presetMode = 'traditional'
                        }
                        rfc4180PresetMode {
                            presetMode = 'rfc4180'
                        }
                        unknownPresetMode {
                            presetMode = 'unknown'
                        }
                    }

                    datasets {
                        traditionalPresetMode {
                            presetMode = 'traditional'
                        }
                        rfc4180PresetMode {
                            presetMode = 'rfc4180'
                        }
                        unknownPresetMode {
                            presetMode = 'unknown'
                        }
                    }
                }
            }.fileName

            def con = csvConnection { config = 'traditionalPresetMode' }
            def file = csv { config = 'traditionalPresetMode' }
            configuration {
                load configName
            }

            assertTrue(con.escaped)
            assertTrue(file.escaped)

            con.config = 'rfc4180PresetMode'
            assertFalse(con.escaped)

            file.config = 'rfc4180PresetMode'
            assertFalse(file.escaped)

            testCase {
                shouldFail { con.presetMode = 'unknown' }
                shouldFail { file.presetMode = 'unknown' }

                shouldFail { con.config = 'unknownPresetMode' }
                shouldFail { file.presetMode = 'unknownPresetMode' }
            }
        }
    }

    @Test
    void testAppendInThreads() {
        Getl.Dsl(this) {
            csvTemp('test_append_threads', true) {
                append = true
                header = true
                field('num') { type = integerFieldType }
            }

            thread {
                useList ((1..5).toList())
                countProc = 3
                run { elem ->
                    rowsTo(csvTemp('test_append_threads')) {
                        writeRow { add ->
                            (1..1000).each {
                                add num: counter.nextCount()
                            }
                        }
                    }
                }
            }
            assertEquals(1000 * 5, csvTemp('test_append_threads').countRow())
        }
    }

    @Test
    void testSkipRows() {
        Getl.Dsl(this) {
            def csv = csvTemp {
                header = true
                fieldDelimiter = ','
                field('id') { type = integerFieldType }
                field('name')
                field('value') { type = numericFieldType; length = 12; precision = 2 }
            }
            textFile(csv.fullFileName()) {
                writeln 'id,name,value'
                writeln 'bad,,1,,string'
                writeln 'bad2string'
                writeln '1,"name",123.45'
                writeln '2,"name",678.90'
            }

            csv.readOpts().skipRows = null
            this.shouldFail { csv.rows() }

            csv.readOpts().skipRows = 2
            def rows = csv.rows()
            assertEquals(2, rows.size())
            assertEquals(1, rows[0].id)
            assertEquals('name', rows[0].name)
            assertEquals(123.45, rows[0].value)
            assertEquals(2, rows[1].id)
            assertEquals('name', rows[1].name)
            assertEquals(678.90, rows[1].value)

            rows = csv.rows(limit: 1)
            assertEquals(1, rows.size())
            assertEquals(1, rows[0].id)
            assertEquals('name', rows[0].name)
            assertEquals(123.45, rows[0].value)

            csv.field.clear()
            csv.retrieveFields()
            assertEquals(3, csv.field.size())
        }
    }
}