package getl.utils

import getl.proc.Executor
import getl.stat.ProcessTime
import groovy.json.StringEscapeUtils
import groovy.transform.CompileStatic
import org.junit.Test

/**
 * @author Alexsey Konstantinov
 */
class StringUtilsTest extends getl.test.GetlTest {
    @Test
    void testToSnakeCase() {
        assertEquals('test_snake_case', StringUtils.ToSnakeCase('TestSnakeCase'))
    }

    @Test
    void testToCamelCase() {
        assertEquals('TestSnakeCase', StringUtils.ToCamelCase('test_snake_case', true))
    }

    @Test
    void testGeneratePassword() {
        assertEquals(10, StringUtils.GeneratePassword(10).length())
    }

    @Test
    void testSetValueString() {
        assertEquals('begin text 123 1.23 end', StringUtils.SetValueString('begin {var1} {var2} {var3} end', [var1: 'text', var2: 123, var3: 1.23]))
        shouldFail { StringUtils.SetValueString('{var1}', [var1: null]) }
    }

    @Test
    void testEvalString() {
        assertEquals('begin text end', StringUtils.EvalString("'begin ' + 'text' + ' end'"))
    }

    @Test
    void testEvalMacroString() {
        assertEquals('begin text 123 2017-02-01 01:02:03 end', StringUtils.EvalMacroString('begin {var1} {var2} {var3} end', [var1: 'text', var2: 123, var3: DateUtils.ParseDateTime('2017-02-01 01:02:03.000')]))
        shouldFail { StringUtils.EvalMacroString('{var1}', [:]) }
        assertEquals('{var1}', StringUtils.EvalMacroString('{var1}', [:], false))
        assertEquals('test str', StringUtils.EvalMacroString('test str', [var1: 'text'], true))
        assertEquals('', StringUtils.EvalMacroString('', [:], true))
        assertNull(StringUtils.EvalMacroString(null, [:]))
        assertEquals('C:\\dir1\\file1.txt', StringUtils.EvalMacroString('{drive}{div}{dir}{div}{file}.{ext}',
                [div: '\\', drive:'C:', dir: 'dir1', file: 'file1', ext: 'txt']))
    }

    @Test
    void testAddLedZeroStr() {
        assertEquals('00123', StringUtils.AddLedZeroStr('123', 5))
    }

    @Test
    void testReplicate() {
        assertEquals('123123123', StringUtils.Replicate('123', 3))
    }

    @Test
    void testLeftStr() {
        assertEquals('123', StringUtils.LeftStr('12345', 3))
    }

    @Test
    void testCutStr() {
        assertEquals('123', StringUtils.CutStr('1234567', 3))
        assertEquals('12 ...', StringUtils.CutStr('1234567', 6))
    }

    @Test
    void testRightStr() {
        assertEquals('345', StringUtils.RightStr('12345', 3))
    }

    @Test
    void testProcessParams() {
        assertEquals('begin text end', StringUtils.ProcessParams('begin ${var1} end', [var1: 'text']))
    }

    @Test
    void testProcessAssertionError() {
        try {
            assert 1 == 0, "1 not equal 0"
        }
        catch (AssertionError e) {
            assertEquals('1 not equal 0. Expression: (1 == 0)', StringUtils.ProcessAssertionError(e))
        }
    }

    @Test
    void testEscapeJava() {
        assertEquals('begin\\n\\"text\\ttest\\"\\nend', StringUtils.EscapeJava('begin\n"text\ttest"\nend'))
    }

    @Test
    void testUnescapeJava() {
        assertEquals('begin\ntext\ttest\nend', StringUtils.UnescapeJava('begin\\ntext\\ttest\\nend'))
    }

    @Test
    void testEscapeJavaWithoutUTF() {
        assertEquals("begin\\n\\'text\\'\t\\\"test\\\"\\nend", StringUtils.EscapeJavaWithoutUTF('begin\n\'text\'\t"test"\nend'))
    }

    @Test
    void testUnescapeJavaWithoutUTF() {
        assertEquals('begin\n\'text\'\t"test"\nend', StringUtils.UnescapeJavaWithoutUTF("begin\\n\\'text\\'\t\\\"test\\\"\\nend"))
    }

    @Test
    void testTransformObjectName() {
        assertEquals('1_2_3_4_5_6_7_8', StringUtils.TransformObjectName('"1".\'2\'-3 4(5)6[7]8'))
    }

    @Test
    void testRandomStr() {
        assertNotNull(StringUtils.RandomStr())
    }

    @Test
    void testDelimiter2SplitExpression() {
        def l = '1[split]2[split]3[split]'.split(StringUtils.Delimiter2SplitExpression('[split]'))
        assertArrayEquals(['1', '2', '3'].toArray(), l)
    }

    @Test
    void testRawToHex() {
        assertEquals('3132333435', StringUtils.RawToHex('12345'.bytes))
    }

    @Test
    void testHexToRaw() {
        assertArrayEquals('12345'.bytes, StringUtils.HexToRaw('3132333435'))
    }

    @Test
    void testNewLocale() {
        assertEquals('RU', StringUtils.NewLocale('ru-RU').country)
    }

    @Test
    void testExtractParentFromChild() {
        assertEquals('\\123-456.~$%789\\_\\123-456',
                StringUtils.ExtractParentFromChild('\\123-456.~$%789\\_\\123-456.~$%789\\_', '.~$%789\\_'))
    }

    @Test
    @CompileStatic
    void testEscapePerfomance() {
        def keys = ['\\': '\\\\', '"': '\\"', '\'':'\\\'' ,'\n': '\\n']
        def str = '123\\456"789\'\n'
        assertEquals('123\\\\456\\"789\\\'\\n',
                StringUtils.ReplaceMany(str, keys))

        assertEquals('\\n',
                StringUtils.ReplaceMany('\n', keys))

        assertEquals('\\\\\\n\\"',
                StringUtils.ReplaceMany('\\\n"', keys))

        def perfCount = 100000
        str = StringUtils.Replicate(str, 10)

        def e = new Executor()
        e.useList((1..10).toList())
        def pt1 = new ProcessTime(name: "Perfomance EscapeJava")
        e.run {
            (1..perfCount).each {
                def r = StringEscapeUtils.escapeJavaScript(str)
            }
        }
        pt1.finish()

        def pt2 = new ProcessTime(name: "Perfomance EscapeJavaWithoutUtf")
        e.run {
            (1..perfCount).each {
                def r = StringUtils.EscapeJavaWithoutUTF(str)
            }
        }
        pt2.finish()

        def resStr = StringUtils.EscapeJavaWithoutUTF(str)

        def pt3 = new ProcessTime(name: "Perfomance UnescapeJava")
        e.run {
            (1..perfCount).each {
                def r = StringUtils.UnescapeJava(resStr)
            }
        }
        pt3.finish()

        def pt4 = new ProcessTime(name: "Perfomance UnescapeJavaWithoutUtf")
        e.run {
            (1..perfCount).each {
                def r = StringUtils.UnescapeJavaWithoutUTF(resStr)
            }
        }
        pt4.finish()
    }

    @Test
    void testRemoveSQLComments() {
        def s = '''-- Comment
/*
    Comment
*/
SELECT /* Comment */ id 
FROM table -- Comment
ORDER BY id /* 
  Comment
*/
'''
        def r = '''SELECT  id 
FROM table 
ORDER BY id'''

        s = StringUtils.RemoveSQLComments(s)
        assertEquals(r.trim(), s.trim())
    }

    @Test
    void testRemoveSQLCommentsWithoutHints() {
        def s = '''/*:count*/
SELECT /* Comment */ id 
FROM table /*+direct*/ -- comment
ORDER BY id /* 
  Comment
*/
'''
        def r = '''/*:count*/
SELECT  id 
FROM table /*+direct*/ 
ORDER BY id'''

        s = StringUtils.RemoveSQLCommentsWithoutHints(s)
        assertEquals(r.trim(), s.trim())
    }

    @Test
    void testDetectStartSQLCommand() {
        def s = '''/*
-- test
*/
-- Test
SET SELECT * FROM table; -- test
/* all test */'''

        def i = StringUtils.DetectStartSQLCommand(s)
        assertEquals('SET', s.substring(i, i + 3))
    }

    @Test
    void testCutStrByWord() {
        assertEquals( '1234', StringUtils.CutStrByWord('1234/5678', 4))
        assertEquals( '1234/', StringUtils.CutStrByWord('1234/5678', 5))
        assertEquals( '1234/', StringUtils.CutStrByWord('1234/5678', 7))
        assertEquals( '1234/5678', StringUtils.CutStrByWord('1234/5678', 9))

        assertEquals( '1 22 333 4444', StringUtils.CutStrByWord('1 22 333 4444', 13))
        assertEquals( '1 22', StringUtils.CutStrByWord('1 22 333 4444', 5))
        assertEquals( '1 22', StringUtils.CutStrByWord('1 22 333 4444', 6))
    }

    @Test
    void testQuoteObjectName() {
        assertNull(StringUtils.ProcessObjectName(null, null))
        assertEquals('a', StringUtils.ProcessObjectName('a'))
        assertEquals('"a"', StringUtils.ProcessObjectName('a', true))
        assertEquals('a?.b', StringUtils.ProcessObjectName('a.b', false, true))
        assertEquals('"a"."b"', StringUtils.ProcessObjectName('a.b', true))
        assertEquals('"a"?."b"', StringUtils.ProcessObjectName('a.b', true, true))
        assertEquals('"a"?."\\"b\\""', StringUtils.ProcessObjectName('a."b"', true, true))
        assertEquals('"a"[0]."b"[0]', StringUtils.ProcessObjectName('a[0].b[0]', true))
        assertEquals('"a"[0]?."b"[0]', StringUtils.ProcessObjectName('a[0].b[0]', true, true))
    }

    @Test
    void testEncrypt() {
        String text = 'Original text from testing method encryption and decryption'
        String password = 'Test unit password'
        def encrypt = StringUtils.Encrypt(text, password)
        assertEquals('CBDE4E89C229A963F4C63CD1626E3DFCFDDBDE61BEDD4658BEC59F10E87984E38651DC259325A88FECF3FEC65B17889499681A3D57D187CAEEAB8F444743926A', encrypt)
        def result = StringUtils.Decrypt(encrypt, password)
        assertEquals(text, result)
    }
}