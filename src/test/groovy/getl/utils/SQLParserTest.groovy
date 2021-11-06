package getl.utils

import getl.test.GetlTest
import org.junit.Test

/**
 * @author Alexsey Konstantinov
 */
class SQLParserTest extends GetlTest {
    @Test
    void testInsertStatement() {
        def sql = '''
INSERT INTO "Schema"."table" ("field1", field2, field3, field4, Field5) VALUES (1, '123', TO_DATE('2016-10-15'), null, DEFAULT);
'''
        def parser = new SQLParser(sql)
        assertEquals(SQLParser.StatementType.INSERT, parser.statementType())

        def res = parser.parseInsertStatement()
        assertEquals('Schema', res.schemaName)
        assertEquals('table', res.tableName)

        def values = [
                field1:[type: Lexer.TokenType.NUMBER, value:1, delimiter:[type: Lexer.TokenType.COMMA, value:',']],
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
        def parser = new SQLParser(sql)
        assertEquals(SQLParser.StatementType.UPDATE, parser.statementType())

        def res = parser.parseUpdateStatement()
        assertEquals('Schema', res.schemaName)
        assertEquals('table', res.tableName)

        def values = [
                field2:[type:Lexer.TokenType.NUMBER, value:123, delimiter:[type:Lexer.TokenType.COMMA, value:',']],
                field3:[type:Lexer.TokenType.FUNCTION, value:'TO_DATE',
                        list:[
                                [type:Lexer.TokenType.QUOTED_TEXT, quote:'\'', value:'2016-10-15']
                        ],
                        start:'(', finish:')', delimiter:[type:Lexer.TokenType.COMMA, value:',']
                ],
                field4:[type:Lexer.TokenType.SINGLE_WORD, value:'null']
        ]
        assertEquals(values, res.values)

        def where = [field1:[[type:Lexer.TokenType.NUMBER, value:1]]]
        assertEquals(where, res.where)
    }

    @Test
    void testDeleteStatement() {
        def sql = '''
DELETE FROM "Schema"."table"
WHERE field1 = 1 AND field2 = '123';
'''
        def lexer = new Lexer(sql, Lexer.sqlScriptType)
        def parser = new SQLParser(sql)
        assertEquals(SQLParser.StatementType.DELETE, parser.statementType())

        def res = parser.parseDeleteStatement()
        assertEquals('Schema', res.schemaName)
        assertEquals('table', res.tableName)

        def where = [
                field1:[[type:Lexer.TokenType.NUMBER, value:1]],
                field2:[[type:Lexer.TokenType.QUOTED_TEXT, quote:'\'', value:'123']]]
        assertEquals(where, res.where)
    }

    @Test
    void testSelectStatement() {
        def sql = 'SELECT id, name FROM table1'
        def parser = new SQLParser(sql)
        assertEquals(SQLParser.StatementType.SELECT, parser.statementType())

        sql = 'SELECT id, name INTO table2 FROM table1'
        parser = new SQLParser(sql)
        assertNull(parser.statementType())
    }

    @Test
    void testCreateStatement() {
        def sql = 'CREATE TABLE table1(id int NOT NULL)'
        def parser = new SQLParser(sql)
        assertEquals(SQLParser.StatementType.CREATE, parser.statementType())
    }

    @Test
    void testAlterStatement() {
        def sql = 'ALTER TABLE table1 ALTER id SET DATA TYPE bigint'
        def lexer = new Lexer(sql, Lexer.sqlScriptType)
        def parser = new SQLParser(sql)
        assertEquals(SQLParser.StatementType.ALTER, parser.statementType())
    }

    @Test
    void testDropStatement() {
        def sql = 'DROP TABLE table1 CASCADE'
        def parser = new SQLParser(sql)
        assertEquals(SQLParser.StatementType.DROP, parser.statementType())
    }

    @Test
    void testEchoStatement() {
        def sql = 'ECHO Test 1 2 3'
        def parser = new SQLParser(sql)
        assertEquals(SQLParser.StatementType.GETL_ECHO, parser.statementType())

        sql = '@ECHO Test 1 2 3'
        parser = new SQLParser(sql)
        assertEquals(SQLParser.StatementType.GETL_ECHO, parser.statementType())
    }

    @Test
    void testIfStatement() {
        def sql = 'IF 1 = 1'
        def lexer = new Lexer(sql, Lexer.sqlScriptType)
        def parser = new SQLParser(sql)
        assertEquals(SQLParser.StatementType.GETL_IF, parser.statementType())
    }

    @Test
    void testSetStatement() {
        def sql = 'SET a = 1'
        def parser = new SQLParser(sql)
        assertEquals(SQLParser.StatementType.GETL_SET, parser.statementType())
    }

    @Test
    void testForStatement() {
        def sql = 'FOR SELECT id, name FROM table'
        def lexer = new Lexer(sql, Lexer.sqlScriptType)
        def parser = new SQLParser(sql)
        assertEquals(SQLParser.StatementType.GETL_FOR, parser.statementType())
    }

    @Test
    void testBlockStatement() {
        def sql = 'BLOCK'
        def parser = new SQLParser(sql)
        assertEquals(SQLParser.StatementType.GETL_BLOCK, parser.statementType())
    }

    @Test
    void testExitStatement() {
        def sql = 'EXIT'
        def lexer = new Lexer(sql, Lexer.sqlScriptType)
        def parser = new SQLParser(sql)
        assertEquals(SQLParser.StatementType.GETL_EXIT, parser.statementType())
    }

    @Test
    void testErrorStatement() {
        def sql = 'ERROR Test 1 2 3'
        def parser = new SQLParser(sql)
        assertEquals(SQLParser.StatementType.GETL_ERROR, parser.statementType())
    }

    @Test
    void testLoadPointStatement() {
        def sql = 'LOAD POINT test:point1 TO var1'
        def lexer = new Lexer(sql, Lexer.sqlScriptType)
        def parser = new SQLParser(sql)
        assertEquals(SQLParser.StatementType.GETL_LOAD_POINT, parser.statementType())
    }

    @Test
    void testSavePointStatement() {
        def sql = 'SAVE POINT test:point1 FROM var1'
        def parser = new SQLParser(sql)
        assertEquals(SQLParser.StatementType.GETL_SAVE_POINT, parser.statementType())
    }
}