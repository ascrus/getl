package getl.utils

import getl.data.*
import getl.jdbc.JDBCDriver
import getl.jdbc.TableDataset
import getl.proc.Flow
import getl.stat.ProcessTime
import getl.tfs.TDS
import org.junit.Test

import javax.sql.rowset.serial.SerialBlob
import javax.sql.rowset.serial.SerialClob
import java.sql.Array
import java.sql.Timestamp

/**
 * Created by ascru on 22.11.2016.
 */
class GenerationUtilsTest extends getl.test.GetlTest {
    @Test
    void testGenerateInt() {
        def r = GenerationUtils.GenerateInt()
        assertTrue(r instanceof Integer)
    }

    @Test
    void testGenerateIntWithRange() {
        def r = GenerationUtils.GenerateInt(1, 1000)
        assertNotNull(r)
        assertTrue(r instanceof Integer)
    }

    @Test
    void testGenerateLong() {
        def r = GenerationUtils.GenerateLong()
        assertNotNull(r)
        assertTrue(r instanceof Long)
    }

    @Test
    void testGenerateString() {
        def r = GenerationUtils.GenerateString(100)
        assertNotNull(r)
        assertTrue(r instanceof String)
        assertTrue(r.length() <= 100 && r.length() > 0)
    }

    @Test
    void testGenerateStringValue() {
        def r = GenerationUtils.GenerateStringValue('test')
        assertNotNull(r)
        assertTrue(r instanceof String)
        assertEquals('"test"', r)
        r = GenerationUtils.GenerateStringValue(null)
        assertEquals('null', r)
    }

    @Test
    void testGenerateBoolean() {
        def r = GenerationUtils.GenerateBoolean()
        assertNotNull(r)
        assertTrue(r instanceof Boolean)
    }

    @Test
    void testGenerateDate() {
        def r = GenerationUtils.GenerateDate()
        assertNotNull(r)
        assertTrue(r instanceof Date)

        r = GenerationUtils.GenerateDate(100)
        assertTrue(r instanceof Date)
    }

    @Test
    void testGenerateDateTime() {
        def r = GenerationUtils.GenerateDateTime()
        assertNotNull(r)
        assertTrue(r instanceof Date)

        r = GenerationUtils.GenerateDateTime(100)
        assertTrue(r instanceof Date)
    }

    @Test
    void testGenerateNumeric() {
        def r = GenerationUtils.GenerateNumeric()
        assertNotNull(r)
        assertTrue(r instanceof BigDecimal)

        r = GenerationUtils.GenerateNumeric(12, 5)
        assertEquals(5, r.scale)

        r = GenerationUtils.GenerateNumeric(5)
        assertEquals(5, r.scale)
    }

    @Test
    void testGenerateDouble() {
        def r = GenerationUtils.GenerateDouble()
        assertNotNull(r)
        assertTrue(r instanceof Double)
    }

    @Test
    void testProcessAlias() {
        def s = 'schema.table.field'
        def r = GenerationUtils.ProcessAlias(s, true)
        assertEquals('"schema"?."table"?."field"', r)

        r = GenerationUtils.ProcessAlias(s, false)
        assertEquals('schema?.table?.field', r)
    }

    @Test
    void testGenerateEmptyValue() {
        Field.Type.values().each { if (it != Field.Type.ROWID) GenerationUtils.GenerateEmptyValue(it, 'test') }
        shouldFail { GenerationUtils.GenerateEmptyValue(Field.Type.ROWID, 'test') }
    }

    @Test
    void testDateFormat() {
        assertEquals('yyyy-MM-dd', GenerationUtils.DateFormat(Field.Type.DATE))
        assertEquals('HH:mm:ss', GenerationUtils.DateFormat(Field.Type.TIME))
        assertEquals('yyyy-MM-dd HH:mm:ss', GenerationUtils.DateFormat(Field.Type.DATETIME))
        shouldFail { GenerationUtils.DateFormat(Field.Type.INTEGER) }
    }

    @Test
    void testGenerateConvertValue() {
        def t = Field.Type.values()
        t.each {
            GenerationUtils.GenerateConvertValue(new Field(name: 'test', type: 'STRING'), new Field(name: 'test', type: it), null, 'row', 'var')
        }
        t.each {
            if (!(it in [Field.Type.DATE, Field.Type.TIME, Field.Type.DATETIME, Field.Type.TIMESTAMP_WITH_TIMEZONE, Field.Type.BLOB, Field.Type.TEXT,
                         Field.Type.OBJECT, Field.Type.ROWID, Field.Type.UUID, Field.Type.ARRAY])) {
                GenerationUtils.GenerateConvertValue(new Field(name: 'test', type: 'INTEGER'), new Field(name: 'test', type: it), null, 'row', 'var')
            }
        }
        t.each {
            if (!(it in [Field.Type.DATE, Field.Type.TIME, Field.Type.DATETIME, Field.Type.TIMESTAMP_WITH_TIMEZONE, Field.Type.BLOB, Field.Type.TEXT,
                         Field.Type.OBJECT, Field.Type.ROWID, Field.Type.UUID, Field.Type.ARRAY])) {
                GenerationUtils.GenerateConvertValue(new Field(name: 'test', type: 'BIGINT'), new Field(name: 'test', type: it), null, 'row', 'var')
            }
        }
        t.each {
            if (!(it in [Field.Type.DATE, Field.Type.TIME, Field.Type.DATETIME, Field.Type.TIMESTAMP_WITH_TIMEZONE, Field.Type.BLOB, Field.Type.TEXT,
                         Field.Type.OBJECT, Field.Type.ROWID, Field.Type.UUID, Field.Type.ARRAY])) {
                GenerationUtils.GenerateConvertValue(new Field(name: 'test', type: 'DOUBLE'), new Field(name: 'test', type: it), null, 'row', 'var')
            }
        }
        t.each {
            if (!(it in [Field.Type.DATE, Field.Type.TIME, Field.Type.DATETIME, Field.Type.TIMESTAMP_WITH_TIMEZONE, Field.Type.BLOB, Field.Type.TEXT,
                         Field.Type.NUMERIC, Field.Type.DOUBLE, Field.Type.OBJECT, Field.Type.ROWID, Field.Type.UUID, Field.Type.ARRAY])) {
                GenerationUtils.GenerateConvertValue(new Field(name: 'test', type: 'BOOLEAN'), new Field(name: 'test', type: it), null, 'row', 'var')
            }
        }
        t.each {
            if (!(it in [Field.Type.DATE, Field.Type.TIME, Field.Type.DATETIME, Field.Type.TIMESTAMP_WITH_TIMEZONE, Field.Type.BLOB, Field.Type.TEXT,
                         Field.Type.OBJECT, Field.Type.ROWID, Field.Type.BOOLEAN, Field.Type.UUID, Field.Type.ARRAY])) {
                GenerationUtils.GenerateConvertValue(new Field(name: 'test', type: 'NUMERIC'), new Field(name: 'test', type: it), null, 'row', 'var')
            }
        }
        t.each {
            if ((it in [Field.Type.STRING, Field.Type.DATE, Field.Type.DATETIME, Field.Type.TIMESTAMP_WITH_TIMEZONE])) {
                GenerationUtils.GenerateConvertValue(new Field(name: 'test', type: 'DATE'), new Field(name: 'test', type: it), null, 'row', 'var')
                GenerationUtils.GenerateConvertValue(new Field(name: 'test', type: 'DATETIME'), new Field(name: 'test', type: it), null, 'row', 'var')
            }
        }
        t.each {
            if ((it in [Field.Type.STRING, Field.Type.TIME])) {
                GenerationUtils.GenerateConvertValue(new Field(name: 'test', type: 'TIME'), new Field(name: 'test', type: it), null, 'row', 'var')
            }
        }
        t.each {
            if ((it in [Field.Type.STRING])) {
                GenerationUtils.GenerateConvertValue(new Field(name: 'test', type: 'TEXT'), new Field(name: 'test', type: it), null, 'row', 'var')
            }
        }
        t.each {
            GenerationUtils.GenerateConvertValue(new Field(name: 'test', type: 'OBJECT'), new Field(name: 'test', type: it), null, 'row', 'var')
			if ((it in [Field.Type.BLOB, Field.Type.STRING])) {
				GenerationUtils.GenerateConvertValue(new Field(name: 'test', type: 'BLOB'), new Field(name: 'test', type: it), null, 'row', 'var')
			}
        }
    }

    @Test
    void testGenerateValue() {
        def t = Field.Type.values()
        t.each { GenerationUtils.GenerateValue(new Field(name: 'test', type: it))}
    }

    @Test
    void testGenerateRowValues() {
        def t = Field.Type.values()
        def f = []
        t.each { f << new Field(name: 'test', type: it) }
        GenerationUtils.GenerateRowValues(f)
    }

    @Test
    void testFields2List() {
        def d = TDS.dataset()
        List<String> s = ['ID', '"DATE"', 'VALUE', '"PRECISION"']
        s.each { name ->
            d.addField Field.New(name.replace('"', ''))
        }
        def f = GenerationUtils.Fields2List(d)
        assertTrue(s.equals(f))
    }

    @Test
    void testGenerateJsonFields() {
        def t = Field.Type.values()
        def f = []
        t.each { f << new Field(name: 'test', type: it, isNull: false) }
        def j = GenerationUtils.GenerateJsonFields(f)
        def n = GenerationUtils.ParseJsonFields(j)
        assertEquals(f, n)
    }

    @Test
    void testEvalGroovyScript() {
        def s = '"test " + \'\\"\' + var + \'\\"\' + " script"'
        assertEquals('test "groovy" script', GenerationUtils.EvalGroovyScript(value: s, vars: [var: 'groovy']))
    }

    @Test
    void testEvalGroovyClosure() {
        def s = '{ var -> "test " + \'\\"\' + var + \'\\"\' + " script" }'
        def c = GenerationUtils.EvalGroovyClosure(s, [var: 'groovy'])
        assertEquals('test "groovy" script', c('groovy'))
    }

    @Test
    void testEvalText() {
        def s = 'test ${var} text'
        assertEquals('test groovy text', GenerationUtils.EvalText(s, [var: 'groovy']))
    }

    @Test
    void testFieldConvertToString() {
        def t = Field.Type.values() - [Field.Type.OBJECT, Field.Type.ARRAY]
        t.each {
            def f = new Field(name: 'test', type: it)
            GenerationUtils.FieldConvertToString(f)
            assertEquals(Field.Type.STRING, f.type)
        }
    }

    @Test
    void testGenerateRowCopyWithMap() {
        def t = Field.Type.values() - [Field.Type.OBJECT, Field.Type.ROWID/*, Field.Type.TEXT*/]
        def c = new TDS().tap { connected = true }
        def l = [] as List<Field>
        def r = [:] as Map<String, Object>
        t.each {
            def f = new Field(name: "field_${it.toString()}", type: it, isNull: false)
            if (f.type in [Field.Type.STRING, Field.Type.TEXT, Field.Type.BLOB, Field.Type.NUMERIC]) f.length = 20
            if (f.type == Field.Type.NUMERIC) f.precision = 5
            l << f
            def v = GenerationUtils.GenerateValue(f)
            switch (it) {
                case Field.Type.BLOB:
                    v = new SerialBlob((byte[])v)
                    break
                case Field.Type.TEXT:
                    v = new SerialClob((v as String).chars)
                    break
                case Field.Type.ARRAY:
                    v = c.currentJDBCDriver.sqlConnect.connection.createArrayOf('INTEGER', (v as List).toArray())
            }
            r.put("field_${it.toString().toLowerCase()}".toString(), v)
        }
        def stat = GenerationUtils.GenerateRowCopy(c.driver as JDBCDriver, l, true)
        def d = [:] as Map<String, Object>
        c.connected = true
        (stat.code as Closure).call(c.javaConnection, r, d)
        r.field_text = r.field_text.getSubString(1L, r.field_text.length() as Integer)
        r.field_blob = r.field_blob.getBytes(1L, r.field_blob.length() as Integer)
        r.field_array = ((int[])(r.field_array as Array).array).toList()
        assertTrue(r.equals(d))
    }

    @Test
    void testGenerateRowCopyWithJDBC() {
		def c = new TDS()

		def t = new TableDataset(connection: c, tableName: 'testRowCopy')
		t.field << new Field(name: 'id', type: 'INTEGER', isKey: true)
		t.field << new Field(name: 'name', length: 50, isNull: false)
		t.field << new Field(name: 'value', type: 'NUMERIC', length: 12, precision: 2, isNull: false)
		t.field << new Field(name: 'value_float', type: 'DOUBLE', isNull: false)
        t.field << new Field(name: 'date', type: 'DATE', isNull: false)
		t.field << new Field(name: 'time', type: 'TIME', isNull: false)
		t.field << new Field(name: 'datetime', type: 'DATETIME', isNull: false)
		t.field << new Field(name: 'data', type: 'BLOB', length: 50, isNull: false)
		t.field << new Field(name: 'text', type: 'TEXT', length: 50, isNull: false)
		t.field << new Field(name: 'uuid', type: 'UUID', isNull: false)
		t.create()

		def n = new TableDataset(connection: c, tableName: 'testRowCopyNew')
		n.field << new Field(name: 'id', type: 'STRING', length: 20, isKey: true)
		n.field << new Field(name: 'name', type: 'TEXT', length: 50, isNull: false)
		n.field << new Field(name: 'value', type: 'DOUBLE', isNull: false)
		n.field << new Field(name: 'value_float', type: 'NUMERIC', length: 12, precision: 2, isNull: false)
		n.field << new Field(name: 'date', type: 'DATETIME', isNull: false)
		n.field << new Field(name: 'time', type: 'DATETIME', isNull: false)
		n.field << new Field(name: 'datetime', type: 'DATE', isNull: false)
		n.field << new Field(name: 'data', type: 'STRING', length: 50, isNull: false)
		n.field << new Field(name: 'text', type: 'STRING', length: 50, isNull: false)
		n.field << new Field(name: 'uuid', type: 'STRING', length: 36, isNull: false)
		n.create()

		try {
			new Flow().writeTo(dest: t) { update ->
				(1..100).each { num ->
					Map r = [
							id: num, name: "name $num", value: num, value_float: num.toDouble(),
							date: GenerationUtils.GenerateDate(),
							time: DateUtils.ClearTime(GenerationUtils.GenerateDateTime()),
							datetime: GenerationUtils.GenerateDateTime(),
							data: GenerationUtils.GenerateString(20).bytes,
							text: GenerationUtils.GenerateString(50),
							uuid: UUID.randomUUID()
					]
					update(r)
				}
			}

			new Flow().copy(source: t, dest: n)
		}
		finally {
			t.drop()
			n.drop()
		}

    }

    @Test
    void testGenerateFieldCopy() {
        def t = Field.Type.values()
        def l = []
        def r = [:]
        t.each {
            def f = new Field(name: "field_${it.toString()}", type: it, isNull: false)
            if (f.type in [Field.Type.STRING, Field.Type.TEXT, Field.Type.BLOB, Field.Type.NUMERIC]) f.length = 20
            if (f.type == Field.Type.NUMERIC) f.precision = 5
            l << f
            r.put("field_${it.toString().toLowerCase()}".toString(), GenerationUtils.GenerateValue(f))
        }
        def c = GenerationUtils.GenerateFieldCopy(l)
        def d = [:]
        c.call(r, d)
        assertEquals(r, d)
    }

    static final int countGenerateRandomRow = 100000

    @Test
    void testGenerateRandomRow() {
        def d = new Dataset()
        d.field('id') { type = bigintFieldType; isNull = false }
        d.field('name') { length = 20; isNull = false }
        d.field('code') { length = 2; isNull = false }
        d.field('is_active') { type = booleanFieldType; isNull = false}
        d.field('percent') { type = integerFieldType; isNull = false; minValue = 0; maxValue = 100 }
        d.field('weight') { type = doubleFieldType; isNull = false}
        d.field('count') { type = bigintFieldType; isNull = false }
        d.field('value') { type = numericFieldType; length = 12; precision = 2; isNull = false}
        d.field('open_date') { type = dateFieldType; isNull = false}
        d.field('insert_time') { type = datetimeFieldType; isNull = false}

        def code = GenerationUtils.GenerateRandomRow(d, /*['id']*/null,
                [
                    _abs_: true,
                    _minValue_: -100,
                    _maxValue_: 100000,
                    id: [identity: true],
                    name: [divLength: 2],
                    code: [list: ['1', '2', '3', '4', '5']],
                    open_date: [date: DateUtils.ParseDate('2019-12-01'), days: 30],
                    insert_time: [date: DateUtils.ParseDate('2019-12-01'), seconds: 3600],
                    weight: [abs: false]
                ])

        (1..10).each {
            Map row = [:]
            code.call(row)

            assertEquals(it, row.id)
            assertNotNull(row.name)
            assertNotNull(row.code)
            assertNotNull(row.is_active)
            assertNotNull(row.percent)
            assertNotNull(row.weight)
            assertNotNull(row.count)
            assertNotNull(row.value)
            assertNotNull(row.open_date)
            assertNotNull(row.insert_time)

            assertTrue((row.name as String).length() <= 10)
            assertTrue((row.code as String) in ['1', '2', '3', '4', '5'])
            assertTrue(row.percent >= 0 && row.percent <= 100)
            assertTrue(row.count >= 0 && row.count <= 100000)
            assertTrue(row.value >= 0)
            assertTrue(row.open_date >= DateUtils.ParseDate('2019-12-01') && row.open_date <= DateUtils.ParseDate('2019-12-31'))
            assertTrue(row.insert_time >= DateUtils.ParseDateTime('2019-12-01 00:00:00.000') && row.insert_time <= DateUtils.ParseDateTime('2019-12-01 01:00:00.000'))
        }

        def pt = new ProcessTime(name: "Generate random $countGenerateRandomRow rows")
        (1..countGenerateRandomRow).each {
            Map row = [id: it]
            code.call(row)
        }
        pt.finish(countGenerateRandomRow)
    }

    @Test
    void testGenerateCalculateMapClosure() {
        def map = [
                field1: 'field1',
                '**field2_1': '${source.field2 + \'!\'}',
                '*field2': '${source.field1.toUpperCase()}',
                field2: '${source.field2_1}',
                field3: '${vars.field3.toLowerCase()}'
        ]

        def cl = GenerationUtils.GenerateCalculateMapClosure(map)
        assertEquals([field1: 'field1'], map)
        def source = [field1: 'test']
        def dest = [:]
        def vars = [field3: 'TEST']
        cl(source, dest, vars)
        assertNull(dest.field1)
        assertEquals('TEST', source.field2)
        assertEquals('TEST!', source.field2_1)
        assertEquals('TEST!', dest.field2)
        assertEquals('test', dest.field3)
    }
}