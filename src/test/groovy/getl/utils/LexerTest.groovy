package getl.utils

import getl.test.GetlTest
import org.junit.Test

/**
 * @author Alexsey Konstantinov
 */
class LexerTest extends GetlTest {
    def writeResult = false

    @Test
    void testParse() {
        def example = '''
static public int test (def param1, def param2) {
    int res = -1
    if (param1 != null) {
        def list = [param1, param2]
        res = list.find { it == 100 }
    }
    else {
        if (param2 != null && param2 == 100) res = 1
    }

    return res
}
'''
        def lexer = new Lexer(example)

        def res = FileUtils.FileFromResources('/utils/lexer_parse.json').text
        if (writeResult)
            new File('d:/send/lexer_parse.json').text = lexer.toString()
        assertEquals(res, lexer.toString())
    }

    @Test
    void testMath() {
        def example = "test+=t"
        def lexer = new Lexer(example)
        assertEquals(['test', '+=', 't'], lexer.tokens*.value)

        example = "test != 't'"
        lexer.input = new StringReader(example)
        lexer.parse()
        assertEquals(['test', '!=', 't'], lexer.tokens*.value)

        example = "test != 't!=0'"
        lexer.input = new StringReader(example)
        lexer.parse()
        assertEquals(['test', '!=', 't!=0'], lexer.tokens*.value)

        example = "test>='t!=0'"
        lexer.input = new StringReader(example)
        lexer.parse()
        assertEquals(['test', '>=', 't!=0'], lexer.tokens*.value)

        example = "test<>'t!=0'"
        lexer.input = new StringReader(example)
        lexer.parse()
        assertEquals(['test', '<>', 't!=0'], lexer.tokens*.value)
    }

    @Test
    void testEmptyQuotes() {
        def example = "test=''"
        def lexer = new Lexer(example)
        assertEquals(['test','=', ''], lexer.tokens*.value)
    }

    @Test
    void testQuotesWithLineBreak() {
        def example = """println("
YEAR
")"""
        def lexer = new Lexer(example)

        def res = '''{
    "tokens": [
        {
            "type": "FUNCTION",
            "value": "println",
            "first": 0,
            "last": 16,
            "list": [
                {
                    "type": "QUOTED_TEXT",
                    "quote": "\\"",
                    "value": "\\nYEAR\\n",
                    "first": 8,
                    "last": 15
                }
            ],
            "start": "(",
            "finish": ")"
        }
    ]
}'''

        assertEquals(res.toString(), lexer.toString())
    }

    @Test
    void testSingleWord() {
        def example = "test=1"
        def lexer = new Lexer(example)
        assertEquals(['test','=', 1], lexer.tokens*.value)

        example = "test"
        lexer = new Lexer(example)
        assertEquals(['test'], lexer.tokens*.value)

        example = "test=test"
        lexer = new Lexer(example)
        assertEquals(['test', '=', 'test'], lexer.tokens*.value)

        example = "test=test\n"
        lexer = new Lexer(example)
        assertEquals(['test', '=', 'test', '\n'], lexer.tokens*.value)
    }

    @Test
    void testOperatorWithThreeChars() {
        def example = "test**=t"
        def lexer = new Lexer(example)
        assertEquals(['test','**=', 't'], lexer.tokens*.value)
    }

    @Test
    void testBracketsAfterOperator() {
        String example = "if(var = 1, or(val = 1, var = 2, (value_1 = 1 and value_2 = 2 and something = 3), onemore = 3), false)"

        def lexer = new Lexer(example)

        lexer.tokens.each { token ->
            List<Map> list = token.list
            list.each {
                if (it.value == 'or') {
                    assertEquals(10, (it.list as List).size())
                }
            }
        }

        example = "var(2), (value_1 = 1 and value_2 = 2 and something = 3)"

        lexer = new Lexer(example)

        lexer.tokens.each { token ->
            List<Map> list = token.list
            list.each {
                if (it.type == 'LIST') {
                    assertEquals(4, (it.list as List).size())
                }
            }
        }
    }

    @Test
    void testOneMore() {
        String example = """
DATE ( 

/*YEAR*/ 

YEAR(EndDate) + FLOOR((MONTH(EndDate) + 3 - 1)/12), 

/*MONTH*/ 

CASE 

(MOD(MONTH(EndDate) + 3, 12 ), 0, 12, MOD(MONTH(EndDate)+ 3, 12 )), 

/*DAY*/ 

(MIN(DAY(EndDate), CASE(MOD(MONTH(EndDate) + 3,12), 9, 30, 4, 30, 6, 30, 11, 30, 2, 

/* return max days for February dependent on if end date is leap year */ 
IF(MOD(YEAR(EndDate) + FLOOR((MONTH(EndDate) + 3)/12), 400) = 0 || (MOD(YEAR(EndDate) + FLOOR((MONTH(EndDate) + 3)/12), 4) = 0 && MOD(YEAR(EndDate) + FLOOR((MONTH(EndDate) + 3)/12), 100) <> 0 ), 29,28), 31)) ))

"""
        def lexer = new Lexer(example)
//        println lexer.toString()

        assertNotNull(lexer.tokens)
    }

    @Test
    void testJavaSyntax() {
        def code = FileUtils.FileFromResources('/utils/lexer_java.txt').text
        def res = FileUtils.FileFromResources('/utils/lexer_java.json').text

        def lexer = new Lexer(code, Lexer.javaScriptType)
        if (writeResult)
          new File('d:/send/lexer_java.json').text = lexer.toString()
        assertEquals(res, lexer.toString())
    }

    @Test
    void testSqlSyntax() {
        def code = FileUtils.FileFromResources('/utils/lexer_sql.txt').text
        def res = FileUtils.FileFromResources('/utils/lexer_sql.json').text

        def lexer = new Lexer(code, Lexer.sqlScriptType)
        if (writeResult)
          new File('d:/send/lexer_sql.json').text = lexer.toString()
        assertEquals(res, lexer.toString())
    }

    @Test
    void testFunctionWithLineBreak() {
        def example = """
Date(

    /*
    YEAR
    */ 

    NewYear(),
    
    /*MONTH*/
    NewMonth() 
)
"""
        def lexer = new Lexer(example)

        assertNotNull(lexer.tokens)
    }

    @Test
    void testKeyWords() {
        def sql = '''
/** Test */
WITH a AS (SELECT 1 FROM dual) -- SELECT * INTO table FROM table
SELECT * 
INTO\ttable 
\tFROM table
'''
        def lexer = new Lexer(sql, Lexer.sqlScriptType)
        assertEquals('WITH a AS SELECT INTO table FROM table', lexer.keyWords())

        sql = '''
SELECT *
FROM table
WHERE a IN (1,2,3)
'''
        lexer = new Lexer(sql, Lexer.sqlScriptType)
        assertEquals('SELECT FROM table WHERE a IN', lexer.keyWords())
    }

    @Test
    void testStatements() {
        def sql = '''
SELECT 1 UNION ALL
SELECT 2;
SELECT 3;
'''
        def lexer = new Lexer(sql, Lexer.sqlScriptType)
        def stats = lexer.statements()
        assertEquals(2, stats.size())
        assertEquals('SELECT UNION ALL SELECT', lexer.KeyWords(stats[0]))
        assertEquals('SELECT', lexer.KeyWords(stats[1]))
    }

    @Test
    void testList() {
        def code = 'i = [a,b,c,d,e]'
        def lexer = new Lexer(code, Lexer.javaScriptType)
        assertEquals('a b c d e', lexer.KeyWords(lexer.list(2)))
    }

    @Test
    void testFunction() {
        def code = 'String func(String name, Integer value) { println \'name:\' + value }'
        def lexer = new Lexer(code, Lexer.javaScriptType)
        assertEquals('String name Integer value', lexer.KeyWords(lexer.function(1).list as List<Map>))
    }

    @Test
    void testObject() {
        def sql = 'SELECT table."id" FROM table'
        def lexer = new Lexer(sql, Lexer.javaScriptType)
        assertEquals(['table', 'id'] as List, lexer.object(1))
    }

    @Test
    void testType() {
        def code = '/* TEST */ String func(String name) { [1,2,3] } // TEST\n["a","b","c"]'
        def lexer = new Lexer(code, Lexer.javaScriptType)
        assertEquals(Lexer.TokenType.COMMENT, lexer.type(0))
        assertEquals(Lexer.TokenType.SINGLE_WORD, lexer.type(1))
        assertEquals(Lexer.TokenType.FUNCTION, lexer.type(2))
        assertEquals(Lexer.TokenType.LIST, lexer.type(3))
        assertEquals(Lexer.TokenType.SINGLE_COMMENT, lexer.type(4))
        assertEquals(Lexer.TokenType.LIST, lexer.type(5))
    }

    @Test
    void testFindByType() {
        def code = '/* TEST */ String func(String name) { [1,2,3] } // TEST\n["a","b","c"]'
        def lexer = new Lexer(code, Lexer.javaScriptType)
        assertEquals(0, lexer.findByType(Lexer.TokenType.COMMENT))
        assertEquals(1, lexer.findByType(Lexer.TokenType.SINGLE_WORD))
        assertEquals(2, lexer.findByType(Lexer.TokenType.FUNCTION))
        assertEquals(3, lexer.findByType(Lexer.TokenType.LIST))
        assertEquals(4, lexer.findByType(Lexer.TokenType.SINGLE_COMMENT))
        assertEquals(5, lexer.findByType(Lexer.TokenType.LIST, 4))
    }

    @Test
    void testToList() {
        def code = '1,2,3;'
        def lexer = new Lexer(code, Lexer.javaScriptType)
        lexer.toList().each { println it }
    }

    @Test
    void testPosition() {
        def lexer = new Lexer('aaa bbb ccc')
        assertEquals([[0, 2], [4, 6], [8, 10]], lexer.tokens.collect { [(it.first as Long).toInteger(), (it.last as Long).toInteger()]} )
        assertEquals('aaa bbb ccc', lexer.script)

        lexer = new Lexer(' aaa \n bbb \n ccc \n')
        assertEquals([[1, 3], [5, 5], [7, 9], [11, 11], [13, 15], [17, 17]], lexer.tokens.collect { [(it.first as Long).toInteger(), (it.last as Long).toInteger()]} )
        assertEquals(' aaa \n bbb \n ccc \n', lexer.script)

        lexer = new Lexer('aaa; bbb; ccc;')
        assertEquals([[0, 2], [3, 3], [5, 7], [8, 8], [10, 12], [13, 13]], lexer.tokens.collect { [(it.first as Long).toInteger(), (it.last as Long).toInteger()]} )
        assertEquals('aaa; bbb; ccc;', lexer.script)

        lexer = new Lexer('aaa(bbb, ccc), bbb')
        assertEquals([[0, 12], [15, 17]], lexer.tokens.collect { [(it.first as Long).toInteger(), (it.last as Long).toInteger()]} )
        assertEquals('aaa(bbb, ccc), bbb', lexer.script)

        lexer = new Lexer('aaa /* bbb */ ccc', Lexer.javaScriptType)
        assertEquals([[0, 2], [4, 12], [14, 16]], lexer.tokens.collect { [(it.first as Long).toInteger(), (it.last as Long).toInteger()]} )
        assertEquals('aaa /* bbb */ ccc', lexer.script)

        lexer = new Lexer('aaa bbb --ccc ddd', Lexer.sqlScriptType)
        assertEquals([[0, 2], [4, 6], [8, 16]], lexer.tokens.collect { [(it.first as Long).toInteger(), (it.last as Long).toInteger()]} )
        assertEquals('aaa bbb --ccc ddd', lexer.script)

        lexer = new Lexer('aaa /* bbb */ ccc /* ddd */', Lexer.sqlScriptType)
        assertEquals([[0, 2], [4, 12], [14, 16], [18, 26]], lexer.tokens.collect { [(it.first as Long).toInteger(), (it.last as Long).toInteger()]} )
        assertEquals('aaa /* bbb */ ccc /* ddd */', lexer.script)

        lexer = new Lexer('aaa, bbb, [1, 2, 3, 4, 5], ccc', Lexer.sqlScriptType)
        assertEquals([[0, 2], [5, 7], [10, 24], [27, 29]], lexer.tokens.collect { [(it.first as Long).toInteger(), (it.last as Long).toInteger()]} )
        assertEquals('aaa, bbb, [1, 2, 3, 4, 5], ccc', lexer.script)

        //println lexer
    }

    @Test
    void testQuotesInText() {
        def code = 'println "text\\"text\\"text"'
        def lexer = new Lexer(code, Lexer.javaScriptType)
        assertEquals('text"text"text', lexer.tokens[1].value)
        assertEquals(8, lexer.tokens[1].first)
        assertEquals(25, lexer.tokens[1].last)

        code = "println 'text\\'text\\'text'"
        lexer = new Lexer(code, Lexer.javaScriptType)
        assertEquals('text\'text\'text', lexer.tokens[1].value)
        assertEquals(8, lexer.tokens[1].first)
        assertEquals(25, lexer.tokens[1].last)

        code = "select 'text''text''text'"
        lexer = new Lexer(code, Lexer.sqlScriptType)
        assertEquals('text\'text\'text', lexer.tokens[1].value)
        assertEquals(7, lexer.tokens[1].first)
        assertEquals(24, lexer.tokens[1].last)

        code = '''ALTER TABLE excel.wag_zalog ADD serial_num_true varchar(100) 
DEFAULT replace(btrim(coalesce(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(
    regexp_substr(wag_zalog.serial_num, E'\\\\b([\\\\d\\\\w]{13})([\\\\d]{4})\\\\b', 1, 1, '', 0), 'А', 'A'), 'В', 'B'), 'С', 'C'), 'Е', 'E'), 'Н', 'H'), 
        'К', 'K'), 'М', 'M'), 'О', 'O'), 'Р', 'P'), 'Т', 'T'), 'Х', 'X'), 'З', '3'), 
    regexp_substr(wag_zalog.item_num, E'(\\\\b[2-9]{1})([0-9]{7})\\\\b', 1, 1, '', 0), 
    regexp_substr(wag_zalog.item_num, E'(\\\\b[0-9]{7})\\\\b', 1, 1, '', 0), 
    regexp_substr(wag_zalog.serial_num, E'(\\\\b[0-9]{7})\\\\b', 1, 1, '', 0), wag_zalog.serial_num)), U&'\\00A0', '');'''
        lexer = new Lexer(code, Lexer.sqlScriptType)
        println lexer
    }

    @Test
    void testScriptBuild() {
        def sql = 'SELECT * FROM table'
        def lexer = new Lexer(sql, Lexer.sqlScriptType)
        assertEquals(sql, lexer.scriptBuild())

        sql = 'SELECT /*comment*/ * FROM table --comment'
        lexer = new Lexer(sql, Lexer.sqlScriptType)
        assertEquals('SELECT  * FROM table ', lexer.scriptBuild(ignoreComments: true))
    }
}