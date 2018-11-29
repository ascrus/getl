package getl.utils

/**
 * @author Alexsey Konstantinov
 */
class StringUtilsTest extends getl.test.GetlTest {
    void testToSnakeCase() {
        assertEquals('test_snake_case', StringUtils.ToSnakeCase('TestSnakeCase'))
    }

    void testToCamelCase() {
        assertEquals('TestSnakeCase', StringUtils.ToCamelCase('test_snake_case', true))
    }

    void testGeneratePassword() {
        assertEquals(10, StringUtils.GeneratePassword(10).length())
    }

    void testSetValueString() {
        assertEquals('begin text 123 1.23 end', StringUtils.SetValueString('begin {var1} {var2} {var3} end', [var1: 'text', var2: 123, var3: 1.23]))
        shouldFail { StringUtils.SetValueString('{var1}', [var1: null]) }
    }

    void testEvalString() {
        assertEquals('begin text end', StringUtils.EvalString("'begin ' + 'text' + ' end'"))
    }

    void testEvalMacroString() {
        assertEquals('begin text 123 2017-02-01 01:02:03.000 end', StringUtils.EvalMacroString('begin {var1} {var2} {var3} end', [var1: 'text', var2: 123, var3: DateUtils.ParseDateTime('2017-02-01 01:02:03.000')]))
        shouldFail { StringUtils.EvalMacroString('{var1}', []) }
    }

    void testAddLedZeroStr() {
        assertEquals('00123', StringUtils.AddLedZeroStr('123', 5))
    }

    void testReplicate() {
        assertEquals('123123123', StringUtils.Replicate('123', 3))
    }

    void testLeftStr() {
        assertEquals('123', StringUtils.LeftStr('12345', 3))
    }

    void testCutStr() {
        assertEquals('123 ...', StringUtils.CutStr('1234567', 3))
    }

    void testRightStr() {
        assertEquals('345', StringUtils.RightStr('12345', 3))
    }

    void testProcessParams() {
        assertEquals('begin text end', StringUtils.ProcessParams('begin ${var1} end', [var1: 'text']))
    }

    void testProcessAssertionError() {
        try {
            assert 1 == 0, "1 not equal 0"
        }
        catch (AssertionError e) {
            assertEquals('1 not equal 0. Expression: (1 == 0)', StringUtils.ProcessAssertionError(e))
        }
    }

    void testEscapeJava() {
        assertEquals('begin\\n\\"text\\ttest\\"\\nend', StringUtils.EscapeJava('begin\n"text\ttest"\nend'))
    }

    void testUnescapeJava() {
        assertEquals('begin\ntext\ttest\nend', StringUtils.UnescapeJava('begin\\ntext\\ttest\\nend'))
    }

    void testEscapeJavaWithoutUTF() {
        assertEquals("begin\\n\\'text\\'\\t\\\"test\\\"\\nend", StringUtils.EscapeJavaWithoutUTF('begin\n\'text\'\t"test"\nend'))
    }

    void testTransformObjectName() {
        assertEquals('1_2_3_4_5_6_7_8', StringUtils.TransformObjectName('"1".\'2\'-3 4(5)6[7]8'))
    }

    void testRandomStr() {
        assertNotNull(StringUtils.RandomStr())
    }

    void testDelimiter2SplitExpression() {
        def l = '1[split]2[split]3[split]'.split(StringUtils.Delimiter2SplitExpression('[split]'))
        assertArrayEquals(['1', '2', '3'].toArray(), l)
    }

    void testRawToHex() {
        assertEquals('3132333435', StringUtils.RawToHex('12345'.bytes))
    }

    void testHexToRaw() {
        assertEquals('12345'.bytes, StringUtils.HexToRaw('3132333435'))
    }

    void testNewLocale() {
        assertEquals('RU', StringUtils.NewLocale('ru-RU').country)
    }
}