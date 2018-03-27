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
                    "type": "SINGLE_WORD",
                    "value": "="
                },
                {
                    "type": "SINGLE_WORD",
                    "value": "-1"
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
                            "type": "FUNCTION",
                            "value": "=",
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
                            ],
                            "start": "[",
                            "finish": "]"
                        },
                        {
                            "type": "SINGLE_WORD",
                            "value": "res"
                        },
                        {
                            "type": "SINGLE_WORD",
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
                                    "type": "SINGLE_WORD",
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
                                    "type": "SINGLE_WORD",
                                    "value": "!="
                                },
                                {
                                    "type": "SINGLE_WORD",
                                    "value": "null"
                                },
                                {
                                    "type": "SINGLE_WORD",
                                    "value": "&&"
                                },
                                {
                                    "type": "SINGLE_WORD",
                                    "value": "param2"
                                },
                                {
                                    "type": "SINGLE_WORD",
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
                            "type": "SINGLE_WORD",
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
        assertEquals('+=', lexer.tokens[1].value)

        example = "test != 't'"
        lexer.input = new StringReader(example)
        lexer.parse()
        assertEquals('!=', lexer.tokens[1].value)

        example = "test != 't!=0'"
        lexer.input = new StringReader(example)
        lexer.parse()
        assertEquals('t!=0', lexer.tokens[2].value)

        example = "test>='t!=0'"
        lexer.input = new StringReader(example)
        lexer.parse()
        assertEquals('>=', lexer.tokens[1].value)

        example = "test<>'t!=0'"
        lexer.input = new StringReader(example)
        lexer.parse()
        assertEquals('<>', lexer.tokens[1].value)
    }

    void testEmptyQuotes() {
        def example = "test=''"
        def lexer = new Lexer(input: new StringReader(example))
        lexer.parse()
        assertEquals('', lexer.tokens[2].value)
    }

    void testSingleWord() {
        def example = "test"
        def lexer = new Lexer(input: new StringReader(example))
        lexer.parse()
        assertEquals('test', lexer.tokens[0].value)
    }
}
