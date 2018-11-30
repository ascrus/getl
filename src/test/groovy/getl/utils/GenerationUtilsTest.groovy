package getl.utils

import getl.data.*
import getl.jdbc.TableDataset
import getl.proc.Flow
import getl.tfs.TDS

import javax.sql.rowset.serial.SerialBlob
import javax.sql.rowset.serial.SerialClob

/**
 * Created by ascru on 22.11.2016.
 */
class GenerationUtilsTest extends getl.test.GetlTest {
    void testGenerateInt() {
        def r = GenerationUtils.GenerateInt()
        assertTrue(r instanceof Integer)
    }

    void testGenerateIntWithRange() {
        def r = GenerationUtils.GenerateInt(1, 1000)
        assertNotNull(r)
        assertTrue(r instanceof Integer)
    }

    void testGenerateLong() {
        def r = GenerationUtils.GenerateLong()
        assertNotNull(r)
        assertTrue(r instanceof Long)
    }

    void testGenerateString() {
        def r = GenerationUtils.GenerateString(100)
        assertNotNull(r)
        assertTrue(r instanceof String)
        assertTrue(r.length() <= 100 && r.length() > 0)
    }

    void testGenerateStringValue() {
        def r = GenerationUtils.GenerateStringValue('test')
        assertNotNull(r)
        assertTrue(r instanceof String)
        assertEquals('"test"', r)
        r = GenerationUtils.GenerateStringValue(null)
        assertEquals('null', r)
    }

    void testGenerateBoolean() {
        def r = GenerationUtils.GenerateBoolean()
        assertNotNull(r)
        assertTrue(r instanceof Boolean)
    }

    void testGenerateDate() {
        def r = GenerationUtils.GenerateDate()
        assertNotNull(r)
        assertTrue(r instanceof Date)

        r = GenerationUtils.GenerateDate(100)
        assertTrue(r instanceof Date)
    }

    void testGenerateDateTime() {
        def r = GenerationUtils.GenerateDateTime()
        assertNotNull(r)
        assertTrue(r instanceof Date)

        r = GenerationUtils.GenerateDateTime(100)
        assertTrue(r instanceof Date)
    }

    void testGenerateNumeric() {
        def r = GenerationUtils.GenerateNumeric()
        assertNotNull(r)
        assertTrue(r instanceof BigDecimal)

        r = GenerationUtils.GenerateNumeric(12, 5)
        assertEquals(5, r.scale)

        r = GenerationUtils.GenerateNumeric(5)
        assertEquals(5, r.scale)
    }

    void testGenerateDouble() {
        def r = GenerationUtils.GenerateDouble()
        assertNotNull(r)
        assertTrue(r instanceof Double)
    }

    void testProcessAlias() {
        def s = 'schema.table.field'
        def r = GenerationUtils.ProcessAlias(s, true)
        assertEquals('"schema"?."table"?."field"', r)

        r = GenerationUtils.ProcessAlias(s, false)
        assertEquals('schema?.table?.field', r)
    }

    void testGenerateEmptyValue() {
        Field.Type.values().each { if (it != Field.Type.ROWID) GenerationUtils.GenerateEmptyValue(it, 'test') }
        shouldFail { GenerationUtils.GenerateEmptyValue(Field.Type.ROWID, 'test') }
    }

    void testDateFormat() {
        assertEquals('yyyy-MM-dd', GenerationUtils.DateFormat(Field.Type.DATE))
        assertEquals('HH:mm:ss', GenerationUtils.DateFormat(Field.Type.TIME))
        assertEquals('yyyy-MM-dd HH:mm:ss', GenerationUtils.DateFormat(Field.Type.DATETIME))
        shouldFail { GenerationUtils.DateFormat(Field.Type.INTEGER) }
    }

    void testGenerateConvertValue() {
        def t = Field.Type.values()
        t.each { GenerationUtils.GenerateConvertValue(new Field(name: 'test', type: 'STRING'), new Field(name: 'test', type: it), null, 'var') }
        t.each {
            if (!(it in [Field.Type.DATE, Field.Type.TIME, Field.Type.DATETIME, Field.Type.BLOB, Field.Type.TEXT,
                         Field.Type.OBJECT, Field.Type.ROWID, Field.Type.UUID])) {
                GenerationUtils.GenerateConvertValue(new Field(name: 'test', type: 'INTEGER'), new Field(name: 'test', type: it), null, 'var')
            }
        }
        t.each {
            if (!(it in [Field.Type.DATE, Field.Type.TIME, Field.Type.DATETIME, Field.Type.BLOB, Field.Type.TEXT,
                         Field.Type.OBJECT, Field.Type.ROWID, Field.Type.UUID])) {
                GenerationUtils.GenerateConvertValue(new Field(name: 'test', type: 'BIGINT'), new Field(name: 'test', type: it), null, 'var')
            }
        }
        t.each {
            if (!(it in [Field.Type.DATE, Field.Type.TIME, Field.Type.DATETIME, Field.Type.BLOB, Field.Type.TEXT,
                         Field.Type.OBJECT, Field.Type.ROWID, Field.Type.UUID])) {
                GenerationUtils.GenerateConvertValue(new Field(name: 'test', type: 'DOUBLE'), new Field(name: 'test', type: it), null, 'var')
            }
        }
        t.each {
            if (!(it in [Field.Type.DATE, Field.Type.TIME, Field.Type.DATETIME, Field.Type.BLOB, Field.Type.TEXT,
                         Field.Type.NUMERIC, Field.Type.DOUBLE, Field.Type.OBJECT, Field.Type.ROWID, Field.Type.UUID])) {
                GenerationUtils.GenerateConvertValue(new Field(name: 'test', type: 'BOOLEAN'), new Field(name: 'test', type: it), null, 'var')
            }
        }
        t.each {
            if (!(it in [Field.Type.DATE, Field.Type.TIME, Field.Type.DATETIME, Field.Type.BLOB, Field.Type.TEXT,
                         Field.Type.OBJECT, Field.Type.ROWID, Field.Type.BOOLEAN, Field.Type.UUID])) {
                GenerationUtils.GenerateConvertValue(new Field(name: 'test', type: 'NUMERIC'), new Field(name: 'test', type: it), null, 'var')
            }
        }
        t.each {
            if ((it in [Field.Type.STRING, Field.Type.DATE, Field.Type.DATETIME])) {
                GenerationUtils.GenerateConvertValue(new Field(name: 'test', type: 'DATE'), new Field(name: 'test', type: it), null, 'var')
                GenerationUtils.GenerateConvertValue(new Field(name: 'test', type: 'DATETIME'), new Field(name: 'test', type: it), null, 'var')
            }
        }
        t.each {
            if ((it in [Field.Type.STRING, Field.Type.TIME])) {
                GenerationUtils.GenerateConvertValue(new Field(name: 'test', type: 'TIME'), new Field(name: 'test', type: it), null, 'var')
            }
        }
        t.each {
            if ((it in [Field.Type.STRING])) {
                GenerationUtils.GenerateConvertValue(new Field(name: 'test', type: 'TEXT'), new Field(name: 'test', type: it), null, 'var')
            }
        }
        t.each {
            GenerationUtils.GenerateConvertValue(new Field(name: 'test', type: 'OBJECT'), new Field(name: 'test', type: it), null, 'var')
			if ((it in [Field.Type.BLOB, Field.Type.STRING])) {
				GenerationUtils.GenerateConvertValue(new Field(name: 'test', type: 'BLOB'), new Field(name: 'test', type: it), null, 'var')
			}
        }
    }

    void testGenerateValue() {
        def t = Field.Type.values()
        t.each { GenerationUtils.GenerateValue(new Field(name: 'test', type: it))}
    }

    void testGenerateRowValues() {
        def t = Field.Type.values()
        def f = []
        t.each { f << new Field(name: 'test', type: it) }
        GenerationUtils.GenerateRowValues(f)
    }

    void testFields2List() {
        def d = TDS.dataset()
        List<String> s = []
        Field.Type.values().each {
            d.field << new Field(name: "FIELD_${it.toString()}", type: it)
            if (it != Field.Type.ROWID) s << "\"FIELD_${it.toString()}\""
        }
        def f = GenerationUtils.Fields2List(d, ['FIELD_ROWID'])
        assertEquals(s, f)
    }

    void testGenerateJsonFields() {
        def t = Field.Type.values()
        def f = []
        t.each { f << new Field(name: 'test', type: it, isNull: false) }
        def j = GenerationUtils.GenerateJsonFields(f)
        def n = GenerationUtils.ParseJsonFields(j)
        assertEquals(f, n)
    }

    void testEvalGroovyScript() {
        def s = '"test " + \'\\"\' + var + \'\\"\' + " script"'
        assertEquals('test "groovy" script', GenerationUtils.EvalGroovyScript(s, [var: 'groovy']))
    }

    void testEvalGroovyClosure() {
        def s = '{ var -> "test " + \'\\"\' + var + \'\\"\' + " script" }'
        def c = GenerationUtils.EvalGroovyClosure(s, [var: 'groovy'])
        assertEquals('test "groovy" script', c('groovy'))
    }

    void testEvalText() {
        def s = 'test ${var} text'
        assertEquals('test groovy text', GenerationUtils.EvalText(s, [var: 'groovy']))
    }

    void testFieldConvertToString() {
        def t = Field.Type.values() - [Field.Type.OBJECT]
        t.each {
            def f = new Field(name: 'test', type: it)
            GenerationUtils.FieldConvertToString(f)
            assertEquals(Field.Type.STRING, f.type)
        }
    }

    void testGenerateRowCopyWithMap() {
        def t = Field.Type.values() - [Field.Type.OBJECT, Field.Type.ROWID]
        def c = new TDS()
        def l = []
        def r = [:]
        t.each {
            def f = new Field(name: "field_${it.toString()}", type: it, isNull: false)
            if (f.type in [Field.Type.STRING, Field.Type.TEXT, Field.Type.BLOB, Field.Type.NUMERIC]) f.length = 20
            if (f.type == Field.Type.NUMERIC) f.precision = 5
            l << f
            def v = GenerationUtils.GenerateValue(f)
            switch (it) {
                case Field.Type.BLOB:
                    v = new SerialBlob(v)
                    break
                case Field.Type.TEXT:
                    v = new SerialClob(v.chars)
                    break
            }
            r.put("field_${it.toString().toLowerCase()}".toString(), v)
        }
        def stat = GenerationUtils.GenerateRowCopy(c.driver, l, true)
        def d = [:]
        c.connected = true
        stat.code.call(c.javaConnection, r, d)
        r."field_text" = r."field_text".getSubString(1L, r."field_text".length() as Integer)
        r."field_blob" = r."field_blob".getBytes(1L, r."field_blob".length() as Integer)
        assertEquals(r, d)
    }

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
}
