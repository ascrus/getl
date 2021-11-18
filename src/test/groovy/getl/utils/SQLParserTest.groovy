package getl.utils

import getl.test.GetlTest
import org.junit.Test

/**
 * @author Alexsey Konstantinov
 */
class SQLParserTest extends GetlTest {
    @Test
    void testInsertStatement() {
        def sql = '''INSERT INTO "Schema"."table" ("field1", field2, field3, field4, Field5) VALUES (1, '123', TO_DATE('2016-10-15'), null, DEFAULT);'''
        def parser = new SQLParser(sql)
        assertEquals(SQLParser.StatementType.INSERT, parser.statementType())

        sql = '''INSERT INTO "Schema"."table" ("field1", field2, field3, field4, Field5) SELECT * FROM table2;'''
        parser = new SQLParser(sql)
        assertEquals(SQLParser.StatementType.INSERT, parser.statementType())
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
    }

    @Test
    void testMergeStatement() {
        def sql = '''
MERGE INTO "Schema"."table1"
USING table2 ON table1.id = table2.id;
'''
        def lexer = new Lexer(sql, Lexer.sqlScriptType)
        def parser = new SQLParser(sql)
        assertEquals(SQLParser.StatementType.MERGE, parser.statementType())
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
        def sql = 'DROP TABLE IF EXISTS table1 CASCADE'
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
        def sql = 'IF (1 = 1) DO { ECHO If complete }'
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
        def sql = 'FOR (SELECT id, name FROM table) DO { ECHO {id}, {name} }'
        def parser = new SQLParser(sql)
        assertEquals(SQLParser.StatementType.GETL_FOR, parser.statementType())

        def posFor = parser.lexer.findFunction('FOR')
        def tokenFor = parser.lexer.tokens[posFor]
        assertEquals('FOR', tokenFor.value)

        def posDo = parser.lexer.findFunction('DO')
        def tokenDo = parser.lexer.tokens[posDo]
        assertEquals('DO', tokenDo.value)
    }

    @Test
    void testBlockStatement() {
        def sql = 'COMMAND { ECHO Native script }'
        def parser = new SQLParser(sql)
        assertEquals(SQLParser.StatementType.GETL_COMMAND, parser.statementType())
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
        def sql = 'LOAD_POINT test:point1 TO var1 WITH MERGE'
        def lexer = new Lexer(sql, Lexer.sqlScriptType)
        def parser = new SQLParser(sql)
        assertEquals(SQLParser.StatementType.GETL_LOAD_POINT, parser.statementType())
    }

    @Test
    void testSavePointStatement() {
        def sql = 'SAVE_POINT test:point1 FROM var1 WITH INSERT'
        def parser = new SQLParser(sql)
        assertEquals(SQLParser.StatementType.GETL_SAVE_POINT, parser.statementType())
    }

    @Test
    void testStatements() {
        def sql = '''@SET var1 = 123;
ECHO var1: ${var1}
SELECT ${var1} AS var1, ';' AS delim, * -- no ;
FROM table;

FOR (
    SELECT id, name
    FROM table1
) DO {
    ECHO {id}, '${name}'
    INSERT INTO table2 VALUES({id}, '{name}');
}

/* 
  IF CONDITION 
*/
IF ({var1} = 123) DO {
    DELETE FROM table3 WHERE id = {var1};
    ECHO Record deleted.
}
'''
        def parser = new SQLParser(sql)
        def scripts = parser.scripts()
        parser.scripts().each {println '------\n' + it }

        def lines = sql.readLines()
        assertEquals(lines[0], scripts[0] + ';')
        assertEquals(lines[1], scripts[1])
        assertEquals(lines.subList(2, 4).join('\n'), scripts[2] + ';')
        assertEquals(lines.subList(5, 12).join('\n'), scripts[3])
        assertEquals(lines.subList(13, 20).join('\n'), scripts[4])
    }

    @Test
    void testTranStatement() {
        def sql = 'START TRANSACTION;'
        def parser = new SQLParser(sql)
        assertEquals(SQLParser.StatementType.START_TRANSACTION, parser.statementType())

        sql = 'BEGIN TRANSACTION;'
        parser = new SQLParser(sql)
        assertEquals(SQLParser.StatementType.START_TRANSACTION, parser.statementType())

        sql = 'START TRAN;'
        parser = new SQLParser(sql)
        assertEquals(SQLParser.StatementType.START_TRANSACTION, parser.statementType())

        sql = 'BEGIN TRAN;'
        parser = new SQLParser(sql)
        assertEquals(SQLParser.StatementType.START_TRANSACTION, parser.statementType())

        sql = 'COMMIT;'
        parser = new SQLParser(sql)
        assertEquals(SQLParser.StatementType.COMMIT, parser.statementType())

        sql = 'ROLLBACK;'
        parser = new SQLParser(sql)
        assertEquals(SQLParser.StatementType.ROLLBACK, parser.statementType())
    }

    @Test
    void testComments() {
        def sql = FileUtils.FileFromResources('/utils/comments.sql').text
        def parser = new SQLParser(sql)
        def scripts = parser.scripts()
        assertEquals(1, scripts.size())
        assertEquals(SQLParser.StatementType.DROP, new SQLParser(scripts[0]).statementType())
    }
}