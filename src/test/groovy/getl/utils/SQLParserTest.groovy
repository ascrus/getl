package getl.utils

import getl.exception.ExceptionParser
import org.junit.Test

/**
 * @author Alexsey Konstantinov
 */
class SQLParserTest extends getl.test.GetlTest {
    @Test
    void testInsertStatement() {
        def sql = '''
INSERT INTO "Schema"."table" ("field1", field2, field3, field4, Field5) VALUES (1, '123', TO_DATE('2016-10-15'), null, DEFAULT);
'''
        def lexer = new Lexer(input: new StringReader(sql))
        lexer.parse()

        def parser = new SQLParser(lexer: lexer)
        assertEquals(SQLParser.StatementType.INSERT, parser.statementType(lexer.tokens))

        def res = parser.parseInsertStatement(lexer.tokens)
        assertEquals('Schema', res.schemaName)
        assertEquals('table', res.tableName)

        def values = [
                field1:[type: Lexer.TokenType.SINGLE_WORD, value:'1', delimiter:[type: Lexer.TokenType.COMMA, value:',']],
                field2:[type: Lexer.TokenType.QUOTED_TEXT, quote:'\'', value:'123', delimiter:[type: Lexer.TokenType.COMMA, value:',']],
                field3:[type: Lexer.TokenType.FUNCTION, value:'TO_DATE',
                        list:[
                                [type: Lexer.TokenType.QUOTED_TEXT, quote:'\'', value:'2016-10-15']
                        ],
                        start:'(', finish:')', delimiter:[type: Lexer.TokenType.COMMA, value:',']],
                field4:[type: Lexer.TokenType.SINGLE_WORD, value: 'null', delimiter:[type: Lexer.TokenType.COMMA, value:',']],
                Field5:[type: Lexer.TokenType.SINGLE_WORD, value:'DEFAULT']
        ]
        assertEquals(values, res.values)
    }

    @Test
    void testUpdateStatement() {
        def sql = '''
UPDATE "Schema"."table"
SET field2 = 123, field3 = TO_DATE('2016-10-15'), "field4" = null
WHERE field1 = 1;
'''
        def lexer = new Lexer(input: new StringReader(sql))
        lexer.parse()

        def parser = new SQLParser(lexer: lexer)
        assertEquals(SQLParser.StatementType.UPDATE, parser.statementType(lexer.tokens))

        def res = parser.parseUpdateStatement(lexer.tokens)
        assertEquals('Schema', res.schemaName)
        assertEquals('table', res.tableName)

        def values = [
                field2:[type:Lexer.TokenType.SINGLE_WORD, value:'123', delimiter:[type:Lexer.TokenType.COMMA, value:',']],
                field3:[type:Lexer.TokenType.FUNCTION, value:'TO_DATE',
                        list:[
                                [type:Lexer.TokenType.QUOTED_TEXT, quote:'\'', value:'2016-10-15']
                        ],
                        start:'(', finish:')', delimiter:[type:Lexer.TokenType.COMMA, value:',']
                ],
                field4:[type:Lexer.TokenType.SINGLE_WORD, value:'null']
        ]
        assertEquals(values, res.values)

        def where = [field1:[[type:Lexer.TokenType.SINGLE_WORD, value:'1']]]
        assertEquals(where, res.where)
    }

    @Test
    void testDeleteStatement() {
        def sql = '''
DELETE FROM "Schema"."table"
WHERE field1 = 1 AND field2 = '123';
'''
        def lexer = new Lexer(input: new StringReader(sql))
        lexer.parse()

        def parser = new SQLParser(lexer: lexer)
        assertEquals(SQLParser.StatementType.DELETE, parser.statementType(lexer.tokens))

        def res = parser.parseDeleteStatement(lexer.tokens)
        assertEquals('Schema', res.schemaName)
        assertEquals('table', res.tableName)

        def where = [field1:[[type:Lexer.TokenType.SINGLE_WORD, value:'1']], field2:[[type:Lexer.TokenType.QUOTED_TEXT, quote:'\'', value:'123']]]
        assertEquals(where, res.where)
    }
}