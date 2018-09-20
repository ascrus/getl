package getl.utils

/**
 * @author Alexsey Konstantinov
 */
class LexerTest extends GroovyTestCase {
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
        def lexer = new Lexer(input: new StringReader(example))
        lexer.parse()

        def res = '''{
    "tokens": [
        {
            "type": "SINGLE_WORD",
            "value": "static"
        },
        {
            "type": "SINGLE_WORD",
            "value": "public"
        },
        {
            "type": "SINGLE_WORD",
            "value": "int"
        },
        {
            "type": "FUNCTION",
            "value": "test",
            "list": [
                {
                    "type": "SINGLE_WORD",
                    "value": "int"
                },
                {
                    "type": "SINGLE_WORD",
                    "value": "res"
                },
                {
                    "type": "OPERATOR",
                    "value": "="
                },
                {
                    "type": "OPERATOR",
                    "value": "-"
                },
                {
                    "type": "SINGLE_WORD",
                    "value": "1"
                },
                {
                    "type": "FUNCTION",
                    "value": "if",
                    "list": [
                        {
                            "type": "SINGLE_WORD",
                            "value": "def"
                        },
                        {
                            "type": "SINGLE_WORD",
                            "value": "list"
                        },
                        {
                            "type": "OPERATOR",
                            "value": "="
                        },
                        {
                            "type": "LIST",
                            "start": "[",
                            "finish": "]",
                            "list": [
                                {
                                    "type": "SINGLE_WORD",
                                    "value": "param1",
                                    "delimiter": {
                                        "type": "COMMA",
                                        "value": ","
                                    }
                                },
                                {
                                    "type": "SINGLE_WORD",
                                    "value": "param2"
                                }
                            ]
                        },
                        {
                            "type": "SINGLE_WORD",
                            "value": "res"
                        },
                        {
                            "type": "OPERATOR",
                            "value": "="
                        },
                        {
                            "type": "FUNCTION",
                            "value": "list.find",
                            "list": [
                                {
                                    "type": "SINGLE_WORD",
                                    "value": "it"
                                },
                                {
                                    "type": "OPERATOR",
                                    "value": "=="
                                },
                                {
                                    "type": "SINGLE_WORD",
                                    "value": "100"
                                }
                            ],
                            "start": "{",
                            "finish": "}"
                        }
                    ],
                    "start": "{",
                    "finish": "}"
                },
                {
                    "type": "FUNCTION",
                    "value": "else",
                    "list": [
                        {
                            "type": "FUNCTION",
                            "value": "if",
                            "list": [
                                {
                                    "type": "SINGLE_WORD",
                                    "value": "param2"
                                },
                                {
                                    "type": "OPERATOR",
                                    "value": "!="
                                },
                                {
                                    "type": "SINGLE_WORD",
                                    "value": "null"
                                },
                                {
                                    "type": "OPERATOR",
                                    "value": "&&"
                                },
                                {
                                    "type": "SINGLE_WORD",
                                    "value": "param2"
                                },
                                {
                                    "type": "OPERATOR",
                                    "value": "=="
                                },
                                {
                                    "type": "SINGLE_WORD",
                                    "value": "100"
                                }
                            ],
                            "start": "(",
                            "finish": ")"
                        },
                        {
                            "type": "SINGLE_WORD",
                            "value": "res"
                        },
                        {
                            "type": "OPERATOR",
                            "value": "="
                        },
                        {
                            "type": "SINGLE_WORD",
                            "value": "1"
                        }
                    ],
                    "start": "{",
                    "finish": "}"
                },
                {
                    "type": "SINGLE_WORD",
                    "value": "return"
                },
                {
                    "type": "SINGLE_WORD",
                    "value": "res"
                }
            ],
            "start": "{",
            "finish": "}"
        }
    ]
}'''

        assertEquals(res, lexer.toString())
    }

    void testMath() {
        def example = "test+=t"
        def lexer = new Lexer(input: new StringReader(example))
        lexer.parse()
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

    void testEmptyQuotes() {
        def example = "test=''"
        def lexer = new Lexer(input: new StringReader(example))
        lexer.parse()
        assertEquals(['test','=', ''], lexer.tokens*.value)
    }

    void testSingleWord() {
        def example = "test=1"
        def lexer = new Lexer(input: new StringReader(example))
        lexer.parse()
        assertEquals(['test','=', '1'], lexer.tokens*.value)

        example = "test"
        lexer = new Lexer(input: new StringReader(example))
        lexer.parse()
        assertEquals(['test'], lexer.tokens*.value)

        example = "test=test"
        lexer = new Lexer(input: new StringReader(example))
        lexer.parse()
        assertEquals(['test', '=', 'test'], lexer.tokens*.value)

        example = "test=test\n"
        lexer = new Lexer(input: new StringReader(example))
        lexer.parse()
        assertEquals(['test', '=', 'test'], lexer.tokens*.value)
    }

    void testOperatorWithThreeChars() {
        def example = "test**=t"
        def lexer = new Lexer(input: new StringReader(example))
        lexer.parse()
        assertEquals(['test','**=', 't'], lexer.tokens*.value)
    }

    void testBracketsAfterOperator() {
        String example = "if(var = 1, or(val = 1, var = 2, (value_1 = 1 and value_2 = 2 and something = 3), onemore = 3), false)"

        def lexer = new Lexer(input: new StringReader(example))
        lexer.parse()

        lexer.tokens.each { token ->
            List<Map> list = token.list
            list.each {
                if (it.value == 'or') {
                    assertEquals(10, (it.list as List).size())
                }
            }
        }

        example = "var(2), (value_1 = 1 and value_2 = 2 and something = 3)"

        lexer = new Lexer(input: new StringReader(example))
        lexer.parse()

        lexer.tokens.each { token ->
            List<Map> list = token.list
            list.each {
                if (it.type == 'LIST') {
                    assertEquals(4, (it.list as List).size())
                }
            }
        }
    }

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
        def lexer = new Lexer(input: new StringReader(example))
        lexer.parse()

        assertNotNull(lexer.tokens)
    }

    void testComments() {
        def example = """
/*IF(ISBLANK( Linked_Reseller__r.ParentId), Linked_Reseller__r.Name, left(Linked_Reseller__r.Parent_Account_Name__c,len(Linked_Reseller__r.Parent_Account_Name__c)-7))
*/

blankvalue(
Linked_Reseller__r.MasterParentId__r.Name,
Linked_Reseller__r.Name
)
"""

        def lexer = new Lexer(input: new StringReader(example))
        lexer.parse()

        assertNotNull(lexer.tokens)
    }

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
        def lexer = new Lexer(input: new StringReader(example))
        lexer.parse()

        assertNotNull(lexer.tokens)
    }
}
