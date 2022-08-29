package getl.csv

import getl.data.Dataset
import getl.data.Field
import getl.lang.Getl
import getl.proc.Flow
import getl.stat.ProcessTime
import getl.test.GetlTest
import getl.tfs.TFS
import getl.utils.DateUtils
import getl.utils.FileUtils
import getl.utils.Logs
import getl.utils.NumericUtils
import getl.utils.StringUtils
import groovy.transform.CompileStatic
import org.junit.BeforeClass
import org.junit.Test

import java.sql.Time

class CSVDriverTest extends GetlTest {
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
            new Field(name: 'Blob', type: 'BLOB', length: 100),
            new Field(name: 'List', type: 'ARRAY')
    ]

    static Map<String, Object> conParams = [path: "${TFS.systemPath}/test_csv", createPath: true, extension: 'csv',
                                            codePage: 'utf-8'] as Map<String, Object>

    @BeforeClass
    static void InitTest() {
        FileUtils.ValidPath(conParams.path as String)
        new File(conParams.path as String).deleteOnExit()
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

    private static void validReadWrite(CSVConnection con, String name) {
        con.autoSchema = true
        def csv = new CSVDataset(connection: con, fileName: name)
        csv.field = fields

        def generateArrayAsList = !csv.isGzFile()
        def generate_row = { id ->
            def row = [:]

            row.id = id
            row.name = "row $id"
            row.date = DateUtils.ParseDate('2016-10-15')
            row.time = Time.valueOf('09:15:30')
            row.datetime = DateUtils.ParseDateTime('2016-10-16 09:15:30.123')
            row.double = 123456789.12
            row.numeric = new BigDecimal(123456789.12)
            row.boolean = true
            row.text = "text \"$id\"\tand\nnew line"
            row.blob = 'abcdef'.bytes
            if (generateArrayAsList)
                row.list = ['a', 'b', 'c']
            else {
                row.list = new String[] { 'a', 'b', 'c' }
            }

            return row
        }

        if (!csv.isGzFile()) {
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
            assertNotNull(row.list)

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
            assertNotNull(row.list)

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
            assertTrue(row.list instanceof List)

            assertEquals(id, row.id)
            assertEquals("row $id".toString(), row.name)
            assertEquals(DateUtils.ParseDate('2016-10-15'), row.date)
            assertEquals(Time.valueOf('09:15:30'), row.time)
            assertEquals(DateUtils.ParseDateTime('2016-10-16 09:15:30.123'), row.datetime)
            assertEquals((123456789.12).doubleValue(), (row.double as Double), 0.toDouble())
            assertEquals(NumericUtils.Round(new BigDecimal(123456789.12), 2), row.numeric)
            assertTrue(row.boolean as Boolean)
            assertEquals("text \"$id\"\tand\nnew line".toString(), row.text)
            assertArrayEquals('abcdef'.bytes, row.blob as byte[])
            assertEquals(['a', 'b', 'c'], row.list)
        }
        assertEquals(2, new_csv.countReadPortions)
        assertEquals(100, new_csv.readRows)

        def unWrite_csv = new CSVDataset(connection: con, fileName: "${name}_unwrite",
                schemaFileName: csv.fullFileSchemaName(), deleteOnEmpty: true)
        unWrite_csv.loadDatasetMetadata()
        unWrite_csv.openWrite(isValid: true)
        def row = generate_row(1)
        row.name = null
        unWrite_csv.write(row)
        shouldFail { unWrite_csv.doneWrite() }
        unWrite_csv.closeWrite()

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

        bad_csv.fieldByName('datetime').tap {
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
        def correct_rows = bad_csv.rows(processError: regError, isValid: true)
        assertEquals(1, correct_rows.size())
        assertTrue((errors.get('2') as String).indexOf('duplicate value') != -1)
        assertTrue((errors.get('3') as String).indexOf('null value encountered') != -1)
        assertTrue((errors.get('4') as String).indexOf('invalid date') != -1)

        con.autoSchema = true
        csv.drop(portions: csv.countWritePortions)

        shouldFail() { unWrite_csv.drop(validExist: true) }
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
1,"123\\"456'789\\"",<NULL>,"123\\"456'789,\\nabc"
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
        new File(ds1.fullFileName()).deleteOnExit()
        ds1.field << new Field(name: 'Id', type: Field.integerFieldType, isKey: true)
        ds1.field << new Field(name: 'Name', length: 50, isNull: false)
        ds1.field << new Field(name: 'Result Time', type: Field.dateFieldType, isNull: false, format: 'dd.MM.yyyy')
        ds1.field << new Field(name: 'Flag', isNull: false, format: 'yes|no')
        ds1.field << new Field(name: 'Value', length: 12)
        new Flow().writeTo(dest: ds1) { updater ->
            (1..3).each { num ->
                Map row = [id: num, name: "name $num", 'result time': DateUtils.ParseDate('2020-12-31'),
                           flag: (num == 1)?'yes':'no', value: (num == 1)?'12 345,67':null]
                updater(row)
            }
        }

        def text1 = new File(ds1.fullFileName()).text
        assertEquals('1,name 1,31.12.2020,yes,"12 345,67"\n2,name 2,31.12.2020,no,\n3,name 3,31.12.2020,no,\n', text1)

        ds1.nullAsValue = '<NULL>'

        def ds2 = new CSVDataset(connection: con, fileName: 'test_row_delimiter_2', rowDelimiter: '\r\n')
        ds2.field = ds1.field
        ds2.field('Result Time') { name = 'result_time'; format = null }
        ds2.field('Flag') { type = booleanFieldType; format = null }
        ds2.field('Value') { type = numericFieldType; precision = 2 }
        new Flow().copy(source: ds1, dest: ds2,
                map: [result_time: 'result time', flag: 'flag;format=yes|no'],
                formatDate: 'dd.MM.yyyy', formatNumeric: 'report_with_comma',
                convertEmptyToNull: true, copyOnlyWithValue: true
        )

        def text2 = new File(ds2.fullFileName()).text
        assertEquals('1,name 1,2020-12-31,true,12345.67\r\n2,name 2,2020-12-31,false,\r\n3,name 3,2020-12-31,false,\r\n', text2)

        ds1.drop()
        ds2.drop()
    }

    static def performanceStringValue = StringUtils.Replicate('0', 23) + '"' + '\n' + "'" + StringUtils.Replicate('0', 23)

    @Test
    void testPerformanceWindows() {
        doPerformance('Windows format with constraints',
                conParams + [escaped: false, codePage: 'cp1251', constraintsCheck: true, nullAsValue: '<NULL>'])
    }

    @Test
    void testPerformanceLinux() {
        doPerformance('Linux format with constraints',
                conParams + [escaped: true, codePage: 'utf-8', constraintsCheck: true, nullAsValue: '<NULL>'])
    }

    @CompileStatic
    private static void doPerformance(String testName, Map testParams) {
		def performanceRows = 50000
		def performanceCols = 50

		Logs.Finest("Test perfomance for $testName ($performanceRows rows, ${performanceCols+2} cols) ...")

		def c = new CSVConnection(testParams + ([autoSchema: false] as Map<String, Object>))
		def t = new CSVDataset(connection: c, fileName: 'test_performance')

		t.field << new Field(name: 'Id', type: Field.Type.INTEGER, isKey: true)
		t.field << new Field(name: 'Name', length: 50, isNull: false)
		(1..performanceCols).each { num ->
            def f = new Field(name: "Value_$num", type: Field.Type.STRING, length: 50, isNull: false)
            if (num % 10 == 0) f.isNull = true
			t.field << f
		}

		try {
			def pt = new ProcessTime(name: "CSV performance write")
			new Flow().writeTo(dest: t) { Closure updater ->
				(1..performanceRows).each { Integer cur ->
					def r = [:] as Map<String, Object>
					r.id = cur
					r.name = performanceStringValue
					(1..performanceCols).each { Integer num ->
                        if (num % 10 == 0)
                            r.put("value_$num".toString(), null as String)
                        else
						    r.put("value_$num".toString(), performanceStringValue)
					}
					updater(r)
				}
			}
            def size = new File(t.fullFileName()).size()
            pt.name = "CSV perfomance write (${FileUtils.SizeBytes(size)})"
			pt.finish(performanceRows as Long)


			pt = new ProcessTime(name: "CSV perfomance read (${FileUtils.SizeBytes(size)})")
			def cur = 0
			new Flow().process(source: t) { Map<String, Object> r ->
				cur++
				assertEquals(cur, r.id)
                assertEquals(performanceStringValue, r.name)
			}
			pt.finish(cur as Long)
			assertEquals(performanceRows, cur)
		}
		finally {
			t.drop()
		}
	}

    @Test
    void testAvailableSplit() {
        Getl.CleanGetl()
        Getl.Dsl(this) {
            CSVDataset csv = csv {
                useConnection csvConnection { path = csvTempConnection().currentPath() }
                fileName = 'test.split'
                field('id') { type = integerFieldType}
                field('name')
                writeOpts {
                    batchSize = 0
                    availableAfterWrite = true
                    splitFile { true }
                    deleteOnEmpty = true
                }
            }

            etl.rowsTo(csv) {
                writeRow { add ->
                    (1..3).each { num ->
                        add id: num, name: "name $num"
                        assertTrue(new File(csv.fullFileName(num)).exists())
                    }
                }
            }

            assertEquals(3, csv.writtenFiles.size() )
            csv.writtenFiles.each {
                assertTrue(new File(it.fileName).exists())
            }

            csv.drop(portions: 3)
            csv.writtenFiles.each {
                assertFalse(new File(it.fileName).exists())
            }
        }
    }

    @Test
    void testNullValue() {
        Getl.CleanGetl()
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
                    etl.rowsTo(ds) {
                        writeRow { add ->
                            add id: 1, name: 'one', v1: 1, v2: '"string"', v3: DateUtils.ParseDate('2019-12-31'), v4: 123.45, v5: true
                            add id: 2, name: 'two'
                        }
                    }
                }

                def read = {
                    etl.rowsProcess(ds) {
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

                    def res = new File((ds as CSVDataset).fullFileName()).getText('utf-8')
                    return res
                }

                escaped = true
                nullAsValue = '\u0001'
                write()
                assertEquals('''id,name,v1,v2,v3,v4,v5
1,"one",1,"\\"string\\"",2019-12-31,123.45,true
2,"two",\u0001,\u0001,\u0001,\u0001,\u0001
''',read())

                escaped = false
                nullAsValue = '\u0002'
                write()
                assertEquals('''id,name,v1,v2,v3,v4,v5
1,one,1,"""string""",2019-12-31,123.45,true
2,two,\u0002,\u0002,\u0002,\u0002,\u0002
''',read())

                escaped = true
                nullAsValue = '\\\\'
                write()
                assertEquals('''id,name,v1,v2,v3,v4,v5
1,"one",1,"\\"string\\"",2019-12-31,123.45,true
2,"two",\\\\,\\\\,\\\\,\\\\,\\\\
''',read())

                escaped = false
                nullAsValue = '\\\\'
                write()
                assertEquals('''id,name,v1,v2,v3,v4,v5
1,one,1,"""string""",2019-12-31,123.45,true
2,two,\\\\,\\\\,\\\\,\\\\,\\\\
''',read())
            }
        }
    }

    @Test
    void testPresetMode() {
        Getl.CleanGetl()
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
            assertTrue(file.escaped())

            con.config = 'rfc4180PresetMode'
            assertFalse(con.escaped)

            file.config = 'rfc4180PresetMode'
            assertFalse(file.escaped())

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
        Getl.CleanGetl()
        Getl.Dsl(this) {
            csvTemp('test_append_threads', true) {
                append = true
                header = false
                field('num') { type = integerFieldType }
                field('date') { type = datetimeFieldType }
                field('name')
                field('value') { type = numericFieldType; length = 12; precision = 3 }
            }

            profile("Generate data to file", 'byte') {
                thread {
                    useList((1..100).toList())
                    countProc = 100
                    run { elem ->
                        etl.rowsTo(csvTemp('test_append_threads')) {
                            writeRow { add ->
                                (1..1000).each {
                                    def num = counter.nextCount()
                                    add num: num, date: new Date(), name: "This $num line", value: num * 0.123
                                }
                            }
                        }
                    }
                }
                countRow = csvTemp('test_append_threads').datasetFile().size()
            }
            assertEquals(1000 * 100, csvTemp('test_append_threads').readLinesCount())
            csvTemp('test_append_threads').datasetFile().text.eachLine {
                assertFalse("Invalid line: $it", it.matches('^(?!\\d+[,].+[,].+[,].+).*$'))
            }
            csvTemp('test_append_threads').eachRow { row ->
                assertNotNull(row.num)
                assertEquals("This ${row.num} line", row.name)
                assertNotNull(row.date)
                assertEquals(row.num * 0.123, row.value)
            }
        }
    }

    @Test
    void testSkipRows() {
        Getl.CleanGetl()
        Getl.Dsl(this) {
            def csv = csvTemp {
                header = true
                fieldDelimiter = ','
                field('id') { type = integerFieldType }
                field('name')
                field('value') { type = numericFieldType; length = 12; precision = 2 }
            }
            textFile(csv.fullFileName()) {
                temporaryFile = true
                writeln 'id,name,value'
                writeln 'bad,,1,,string'
                writeln 'bad2string'
                writeln '1,"name",123.45'
                writeln '2,"name",678.90'
            }

            csv.readOpts.skipRows = null
            shouldFail { csv.rows() }

            csv.readOpts.skipRows = 2
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

    @Test
    void testSaveErrors() {
        Getl.CleanGetl()
        Getl.Dsl(this) {
            def csv = csvTemp {
                header = true
                fieldDelimiter = ','
                field('id') { type = integerFieldType; isNull = false; isKey = true }
                field('name') { isNull = false; length = 10 }
                field('value') { type = numericFieldType; length = 6; precision = 2 }
                constraintsCheck = true
                readOpts { saveErrors = true }
            }
            textFile(csv.fullFileName()) {
                temporaryFile = true
                writeln 'id,name,value'
                writeln '1,test 1,123.45'
                writeln '2,test 2,321.54'
                writeln '1,test 1,123.45'
                writeln ',test 1,123.45'
                writeln '1,test 1,bad'
                writeln '3,12345678901,100.00'
            }
            def rows = csv.rows()
            assertEquals(2, rows.size())
            assertEquals([id:1, name: 'test 1', value: 123.45], rows[0])
            assertEquals([id:2, name: 'test 2', value: 321.54], rows[1])
            def errRows = csv.errorsDataset.rows()
            assertEquals(4, errRows.size())
        }
    }

    @Test
    void testTrim() {
        Getl.Dsl {
            def ds = csvTemp {
                fieldDelimiter = ';'

                field('id') { type = bigintFieldType; trim = true }
                field('name') { trim = true }
                field('pok') { type = integerFieldType; trim = true }
                field('value') { type = numericFieldType; length = 12; precision = 2; trim = true }
                field('kpi') { type = doubleFieldType; trim = true }
                field('flag') { type = booleanFieldType; trim = true }
            }

            textFile(ds.fullFileName()) {
                writeln(' 1 ; aaa ; 123 ; 123.45 ; 123.45 ; true ')
                writeln('  ; aaa ; 123 ; 123.45 ; 123.45 ; true ')
            }

            def rows = ds.rows()

            def r = rows[0]
            assertEquals(1, r.id)
            assertEquals('aaa', r.name)
            assertEquals(123, r.pok)
            assertEquals(123.45, r.value)
            assertEquals(123.45, r.kpi as Double, 0)
            assertTrue(r.flag as Boolean)

            r = rows[1]
            assertNull(r.id)
        }
    }

    @Test
    void testEncode() {
        Getl.Dsl {
            def csv1 = csvTemp {
                useConnection csvTempConnection {
                    header = true
                    fieldDelimiter = ';'
                    extension = 'csv'
                    locale = (IsJava8())?'ru-RU':'ce-RU'
                    formatDate = 'dd MMM yyyy'
                    formatDateTime = 'dd MMM yyyy HH:mm:ss'
                    decimalSeparator = ','
                    groupSeparator = ' '
                    formatBoolean = 'да|нет'
                }
                fileName = 'test_encode1'
                field('id') { type = integerFieldType; isNull = false }
                field('name') { length = 12; isNull = false }
                field('long_id') { type = bigintFieldType; isNull = false }
                field('date') { type = dateFieldType; isNull = false }
                field('ts') { type = datetimeFieldType; isNull = false }
                field('value') { type = numericFieldType; length = 9; precision = 2; isNull = false }
                field('complete') { type = booleanFieldType; isNull = false }
                field('uuid') { type = uuidFieldType; isNull = false }

                textFile(datasetFile()) {
                    temporaryFile = true
                    it.writeln('id;name;long_id;date;ts;value;complete;uuid')
                    it.writeln('1;Наименование;5000000000;31 янв 2021;28 фев 2020 01:02:03;999 123,45;да;123e4567-e89b-12d3-a456-556642440000')
                }

                eachRow {row ->
                    assertEquals(1, row.id)
                    assertEquals('Наименование', row.name)
                    assertEquals(5000000000, row.long_id)
                    assertEquals(DateUtils.ParseDate('2021-01-31'), row.date as Date)
                    assertEquals(DateUtils.ParseDateTime('2020-02-28 01:02:03.000'), row.ts as Date)
                    assertEquals(999123.45, row.value)
                    assertTrue(row.complete as Boolean)
                    assertEquals('123e4567-e89b-12d3-a456-556642440000', row.uuid.toString())
                }
            }

            def csv2 = csvTemp {
                useConnection csvTempConnection()
                header = true
                fieldDelimiter = ';'
                extension = 'csv'
                locale = (IsJava8())?'ru-RU':'ce-RU'
                formatDate = 'dd MMM yyyy'
                formatDateTime = 'dd MMM yyyy HH:mm:ss'
                decimalSeparator = ','
                groupSeparator = ' '
                formatBoolean = 'ДА|НЕТ'
                fileName = 'test_encode2'
                field = csv1.field
                etl.rowsTo {
                    writeRow { add ->
                        csv1.eachRow { row ->
                            add row
                        }
                    }
                }

                eachRow {row ->
                    assertEquals(1, row.id)
                    assertEquals('Наименование', row.name)
                    assertEquals(5000000000, row.long_id)
                    assertEquals(DateUtils.ParseDate('2021-01-31'), row.date as Date)
                    assertEquals(DateUtils.ParseDateTime('2020-02-28 01:02:03.000'), row.ts as Date)
                    assertEquals(999123.45, row.value)
                    assertTrue(row.complete as Boolean)
                    assertEquals('123e4567-e89b-12d3-a456-556642440000', row.uuid.toString())
                }
            }

            assertEquals(csv1.datasetFile().text, csv2.datasetFile().text)

            csv2.field.clear()
            csv2.retrieveFields()
            assertEquals(csv1.field, csv2.field)
        }
    }

    private static void printFields(List<Field> field) {
        field.each { println "${it.name} ${it.type}${(it.length > 0)?"(${it.length}, ${it.precision})":''}" +
                "${(!it.isNull)?' NOT NULL':''}${(it.trim)?' TRIM':''}" }
    }

    @Test
    void testDetectFieldsFromFile1() {
        Getl.Dsl {
            def con = csvConnection {
                path = '{GETL_TEST}/csv'
            }

            csv {
                useConnection con
                fileName = 'test1.csv'
                if (!existsFile())
                    return

                fieldDelimiter = ';'
                decimalSeparator = ','
                groupSeparator = ' '
                uniFormatDateTime = 'dd.MM.yyyy HH:mm:ss'
                quoteStr = '\u0001'

                retrieveFields()
                printFields(field)

                eachRow { row ->
                    assertNotNull(row.id)
                    assertNotNull(row."изменено")
                    assertNotNull(row."название компании")
                    assertNotNull(row."фио")
                    assertNotNull(row."телефон")
                    assertNotNull(row.mail)
                    assertNotNull(row."тип имущества")
                    assertNotNull(row."предмет лизинга")
                    assertNotNull(row."стоимость имущества")
                    assertNotNull(row."валюта сделки")
                    assertNotNull(row."планируемый срок лизинга")
                    assertTrue(row."размер аванса" == null || row."размер аванса" > 0)
                }
            }
        }
    }

    @Test
    void testDetectFieldsFromFile2() {
        Getl.Dsl {
            def con = csvConnection {
                path = '{GETL_TEST}/csv'
            }

            csv {
                useConnection con
                fileName = 'test2.csv'
                if (!existsFile())
                    return

                fieldDelimiter = ';'
                locale = 'ce-RU'
                formatDate = 'dd MMM yyyy'
                formatDateTime = 'dd MMM yyyy HH:mm:ss'
                decimalSeparator = ','
                groupSeparator = ' '
                codePage = 'cp1251'

                retrieveFields()
                printFields(field)
            }
        }
    }

    @Test
    void testDetectFieldsFromFile3() {
        Getl.Dsl {
            def con = csvConnection {
                path = '{GETL_TEST}/csv'
            }

            csv {
                useConnection con
                fileName = 'test3.csv'
                if (!existsFile())
                    return

                header = false
                fieldDelimiter = ';'
                uniFormatDateTime = 'yyyy-MM-dd HH:mm:ss'
                formatBoolean = 't|f'

                retrieveFields()
                printFields(field)
            }
        }
    }

    @Test
    void testReadAdditionalColumns() {
        Getl.Dsl {
            csvTemp {
                header = true
                fieldDelimiter = ','
                field('id') { type = integerFieldType }
                field('name')
                field('value') { type = numericFieldType; length = 6; precision = 2 }

                textFile(datasetFile()) {
                    isTemporaryFile = true
                    writeln('id,name,value,date,description')
                    writeln('1,"Name 1",100.0,2021-01-01,Description 1')
                }
                shouldFail { rows() }

                fieldOrderByHeader = true

                def rl = rows()
                assertEquals(1, rl.size())
                def row = rl[0]
                assertEquals(1, row.id)
                assertEquals('Name 1', row.name)
                assertEquals(100.0, row.value)

                textFile(datasetFile()) {
                    writeln('value,id,name,date,description')
                    writeln('100.0,1,"Name 1",2021-01-01,Description 1')
                }
                rl = rows()
                assertEquals(1, rl.size())

                row = rl[0]
                assertEquals(1, row.id)
                assertEquals('Name 1', row.name)
                assertEquals(100.0, row.value)

                header = false
                textFile(datasetFile()) {
                    writeln('1,"Name 1",100.0,2021-01-01,Description 1')
                }
                rl = rows()
                assertEquals(1, rl.size())
                row = rl[0]
                assertEquals(1, row.id)
                assertEquals('Name 1', row.name)
                assertEquals(100.0, row.value)
            }
        }
    }

    @Test
    void testArrayBracket() {
        Getl.Dsl {
            csvTemp {
                header = true
                fieldDelimiter = '|'
                arrayOpeningBracket = '{'
                arrayClosingBracket = '}'
                field('list') { type = arrayFieldType }

                etl.rowsTo {
                    writeRow { add ->
                        add list: ['1', '2', '3', 4, 5]
                    }
                }

                def str = 'list\n"{""1"",""2"",""3"",4,5}"\n'
                assertEquals(str, datasetFile().text)

                assertEquals(1, countRow())
                def row = rows()[0]
                assertEquals(['1', '2', '3', 4, 5], row.list)
            }
        }
    }

    @Test
    void testLocalData() {
        Getl.Dsl {
            csvTemp { ds ->
                useConnection csvTempConnection {
                    locale = 'ce-RU'
                    formatDateTime = 'dd MMM yyyy HH:mm:ss'
                    formatDate = 'dd MMM yyyy'
                    decimalSeparator = ','
                    fieldDelimiter = ';'
                }
                field('id') { type = integerFieldType }
                field('name')
                field('date') { type = dateFieldType }
                field('value') { type = numericFieldType; length = 15; precision = 12 }
                attachToDataset('1;Тест1;31 дек 2013;100,001\n2;Тест2;23 ноя 2013;200,002\n3;Тест3;1 янв 2014;300,003', ds)
                def i = 0
                eachRow { r ->
                    i++
                    assertEquals(i, r.id)
                    assertEquals('Тест' + i, r.name)
                    assertNotNull(r.date)
                    assertNotNull(r.value)
                    assertEquals(i * 100 + i / 1000, r.value)
                }
                assertEquals(3, readRows)
            }
        }
    }
}