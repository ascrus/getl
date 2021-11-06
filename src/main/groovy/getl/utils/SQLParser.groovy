//file:noinspection unused
package getl.utils

import getl.exception.ExceptionParser
import groovy.transform.CompileStatic

/**
 * SQL parser
 * @author Alexsey Konstantinov
 *
 */
@CompileStatic
class SQLParser {
	/** List of type statements */
	static enum StatementType {
		SELECT, INSERT, UPDATE, DELETE, CREATE, ALTER, DROP,
		GETL_ECHO, GETL_IF, GETL_SET, GETL_FOR, GETL_BLOCK, GETL_EXIT,
		GETL_ERROR, GETL_LOAD_POINT, GETL_SAVE_POINT
	}

	/**
	 * Create new SQL parser
	 * @param lexer script lexer
	 */
	SQLParser(Lexer lexer) {
		this.lexer = lexer
	}

	/**
	 * Create new SQL parser
	 * @param script SQL script
	 */
	SQLParser(String script) {
		this.lexer = new Lexer(script, Lexer.sqlScriptType)
	}
	
	/** Script lexer */
	private Lexer lexer

	/** Script lexer */
	Lexer getLexer() { lexer }

	private Map<String, String> regexp = [
			GETL_ECHO: '(?i)[@]{0,1}ECHO\\s+.+',
			GETL_IF: '(?i)[@]{0,1}IF\\s*.*',
			GETL_SET: '(?i)[@]{0,1}SET\\s+.+',
			GETL_FOR: '(?i)[@]{0,1}FOR\\s+.+',
			GETL_BLOCK: '(?i)[@]{0,1}BLOCK',
			GETL_EXIT: '(?i)[@]{0,1}EXIT',
			GETL_ERROR: '(?i)[@]{0,1}ERROR\\s+.+',
			GETL_LOAD_POINT: '(?i)[@]{0,1}LOAD\\s+POINT\\s+.+TO\\s+.+',
			GETL_SAVE_POINT: '(?i)[@]{0,1}SAVE\\s+POINT\\s+.+FROM\\s+.+',
	        INSERT: '(?i)INSERT\\s+INTO\\s+(.+)\\s+VALUES',
			UPDATE: '(?i)UPDATE\\s+(.+)\\s+SET\\s+.+',
			DELETE: '(?i)DELETE\\s+FROM(.+)\\s+WHERE\\s+.+',
			CREATE: '(?i)CREATE\\s+(TABLE|VIEW|PROCEDURE|FUNCTION|SCHEMA)\\s+.+',
			ALTER: '(?i)ALTER\\s+(TABLE|VIEW|PROCEDURE|FUNCTION|SCHEMA)\\s+.+',
			DROP: '(?i)DROP\\s+(TABLE|VIEW|PROCEDURE|FUNCTION|SCHEMA)\\s+.+',
			SELECT: '(?i).*SELECT (?!(.+ INTO ){1}).+ FROM .+'
	]

	static private final Integer minSizeInsertStatement = 5
	static private final String insertKeyWord = 'INSERT INTO'
	
	static private final Integer minSizeUpdateStatement = 8
	static private final String updateKeyWord = 'UPDATE'
	
	static private final Integer minSizeDeleteStatement = 6
	static private final String deleteKeyWord = 'DELETE FROM'

	@SuppressWarnings('UnnecessaryQualifiedReference')
	StatementType statementType(List tokens = lexer.tokens) {
		if (lexer == null)
			throw new ExceptionParser('Required lexer for parsing!', tokens)
		if (tokens == null)
			throw new ExceptionParser('Lexer not parsing!', tokens)

		StatementType res = null
		def worlds = lexer.keyWords()
		for (item in regexp) {
			if (worlds.matches(item.value)) {
				res = StatementType.valueOf(item.key)
				break
			}
		}

		return res
	}
	
	/**
	 * Parse insert statement
	 * @param tokens
	 * @return
	 */
	Map parseInsertStatement(List<Map> tokens = lexer.tokens) {
		if (lexer == null)
			throw new ExceptionParser('Required lexer for parsing!', tokens)
		
		if (tokens == null || tokens.size() < minSizeInsertStatement)
			throw new ExceptionParser('Invalid insert DML operator!', tokens)
		if (lexer.KeyWords(tokens, 0, 2).toUpperCase() != insertKeyWord)
			throw new ExceptionParser("Expected \"$insertKeyWord\"!", tokens)
		
		def start = 3
		def object = lexer.Object(tokens, 2)
		
		if (object == null || object.isEmpty() || object.size() > 3)
			throw new ExceptionParser('Expected table name!', tokens)
		def tableName = object[object.size() - 1]
		def schemaName = (object.size() > 1)?object[object.size() - 2]:null
		def dbName = (object.size() > 2)?object[object.size() - 3]:null
		
		
		def res = [:]
		res.tableName = tableName
		if (schemaName != null)
			res.schemaName = schemaName
		if (dbName != null)
			res.dbName = schemaName
		
		def fields = lexer.List(tokens, start)
		if (fields == null)
			throw new ExceptionParser('Expected list of field!', tokens)
		
		start++
		def valParams = lexer.Function(tokens, start)
		if (valParams == null || ((String)valParams.value).toUpperCase() != "VALUES")
			throw new ExceptionParser("Expected VALUES!", tokens)
		
		List<String> values = valParams.list as List<String>
		if (fields.size() != values.size())
			throw new ExceptionParser('Number of values does not correspond to the number of specified fields!', tokens)
		def fv = [:]
		res.put("values", fv)
		for (Integer i = 0; i < fields.size(); i++) {
			fv.put(fields[i].value, values[i])
		}
		
		start++
		if (start < tokens.size()) {
			if (lexer.Type(tokens, start) == Lexer.TokenType.SEMICOLON) start++
		}
		if (start < tokens.size()) {
			println tokens[start]
			throw new ExceptionParser("Expected end statement!", tokens)
		}
		
		return res
	}
	
	/**
	 * Parse update statement
	 * @param tokens
	 * @return
	 */
	Map parseUpdateStatement(List<Map> tokens = lexer.tokens) {
		if (lexer == null)
			throw new ExceptionParser("Required lexer for parsing!", tokens)
		
		if (tokens == null || tokens.size() < minSizeUpdateStatement)
			throw new ExceptionParser("Invalid update DML operator!", tokens)
		if (lexer.KeyWords(tokens, 0, 1).toUpperCase() != updateKeyWord)
			throw new ExceptionParser("Expected \"$updateKeyWord\"!", tokens)
		
		def start = 2
		def object = lexer.Object(tokens, 1)
		
		if (object.isEmpty() || object.size() > 3)
			throw new ExceptionParser("Expected table name!", tokens)
		def tableName = object[object.size() - 1]
		def schemaName = (object.size() > 1)?object[object.size() - 2]:null
		def dbName = (object.size() > 2)?object[object.size() - 3]:null
		
		def res = [:]
		res."tableName" = tableName
		if (schemaName != null)
			res."schemaName" = schemaName
		if (dbName != null)
			res."dbName" = schemaName
		
		if (lexer.KeyWords(tokens, start, 1).toUpperCase() != "SET")
			throw new ExceptionParser('Expected "SET" in statement!', tokens)

		start++
		
		def wherePos = lexer.FindKeyWord(tokens, 'WHERE', start)
		if (wherePos == -1)
			throw new ExceptionParser('Expected "WHERE" in statement!', tokens)
		def setFinishPos = wherePos - 1 
		
		Map values = [:]
		res.put("values", values)
		def valueList = lexer.ToList(tokens, start, setFinishPos)
		valueList.each { List<Map> setToken ->
			if (setToken.size() != 3)
				throw new ExceptionParser('Invalid set operator!', setToken)
			def token = setToken[1]
			if (token."type" != Lexer.TokenType.OPERATOR || token.value != '=')
				throw new ExceptionParser('Invalid field in set operator!', setToken)
			token = setToken[0]
			if (!(token."type" == Lexer.TokenType.SINGLE_WORD || (token.type == Lexer.TokenType.QUOTED_TEXT && token.quote == '"')))
				throw new ExceptionParser('Invalid set operator!', setToken)
			values.put(token."value", setToken[2])
		}

		Map where = [:]
		res.where = where
		def whereList = lexer.ToList(tokens, wherePos + 1, tokens.size() - 1, 'AND')
		whereList.each { List<Map> whereToken ->
			def token = whereToken[0]
			if (!(token.type == Lexer.TokenType.SINGLE_WORD || (token.type == Lexer.TokenType.QUOTED_TEXT && token.quote == '"')))
				throw new ExceptionParser('Invalid where operator!', whereToken)
			def fieldName = token."value"
			
			token = whereToken[1]
			def value
			if (token."type" == Lexer.TokenType.OPERATOR && token."value" == "=") {
				value = whereToken.subList(2, whereToken.size())
			} 
			else {
				value = whereToken.subList(1, whereToken.size())
			}
			
			where.put(fieldName, value)
		}
		
		return res
	}

	Map parseDeleteStatement(List<Map> tokens = lexer.tokens) {
		if (lexer == null)
			throw new ExceptionParser('Required lexer for parsing!', tokens)
		
		if (tokens == null || tokens.size() < minSizeDeleteStatement)
			throw new ExceptionParser('Invalid delete DML operator!', tokens)
		if (lexer.KeyWords(tokens, 0, 2).toUpperCase() != deleteKeyWord)
			throw new ExceptionParser("Expected \"$deleteKeyWord\"!", tokens)
		
		def start = 3
		def object = lexer.Object(tokens, 2)
		
		if (object.isEmpty() || object.size() > 3)
			throw new ExceptionParser('Expected table name!', tokens)
		def tableName = object[object.size() - 1]
		def schemaName = (object.size() > 1)?object[object.size() - 2]:null
		def dbName = (object.size() > 2)?object[object.size() - 3]:null
		
		def res = [:]
		res.tableName = tableName
		if (schemaName != null)
			res.schemaName = schemaName
		if (dbName != null)
			res.dbName = schemaName
		
		if (lexer.KeyWords(tokens, start, 1).toUpperCase() != 'WHERE')
			throw new ExceptionParser('Expected "WHERE" in statement!', tokens)
		start++

		Map where = [:]
		res.where = where
		def whereList = lexer.ToList(tokens, start, tokens.size() - 1, 'AND')
		whereList.each { List<Map> whereToken ->
			def token = whereToken[0]
			if (!(token."type" == Lexer.TokenType.SINGLE_WORD || (token.type == Lexer.TokenType.QUOTED_TEXT && token.quote == '"')))
				throw new ExceptionParser('Invalid where operator!', whereToken)

			def fieldName = token.value
			
			token = whereToken[1]
			def value
			if (token.type == Lexer.TokenType.OPERATOR && token.value == '=') {
				value = whereToken.subList(2, whereToken.size())
			}
			else {
				value = whereToken.subList(1, whereToken.size())
			}
			
			where.put(fieldName, value)
		}
		
		return res
	}

	List<List<Map>> scripts() {
		if (lexer == null)
			throw new ExceptionParser('Required lexer for parsing!', null)

		if (lexer.tokens == null)
			throw new ExceptionParser('Lexer not parsing!', null)

		return lexer.statements()
	}
}
