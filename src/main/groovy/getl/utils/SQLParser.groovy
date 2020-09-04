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
	/**
	 * List of type statements
	 */
	static enum StatementType {INSERT, UPDATE, DELETE}
	
	/**
	 * Lexer for parsing
	 */
	Lexer lexer
	
	static private final Integer minSizeInsertStatement = 5
	static private final String insertKeyWord = 'INSERT INTO'
	
	static private final Integer minSizeUpdateStatement = 8
	static private final String updateKeyWord = 'UPDATE'
	
	static private final Integer minSizeDeleteStatement = 6
	static private final String deleteKeyWord = 'DELETE FROM'

	StatementType statementType(List tokens) {
		if (lexer == null) throw new ExceptionParser("Required lexer for parsing", tokens)
		if (tokens == null) throw new ExceptionParser("NULL token", tokens)
		
		if (tokens.size() >= minSizeInsertStatement && lexer.keyWords(tokens, 0, 2).toUpperCase() == insertKeyWord) return StatementType.INSERT
		if (tokens.size() >= minSizeUpdateStatement && lexer.keyWords(tokens, 0, 1).toUpperCase() == updateKeyWord) return StatementType.UPDATE
		if (tokens.size() >= minSizeDeleteStatement && lexer.keyWords(tokens, 0, 2).toUpperCase() == deleteKeyWord) return StatementType.DELETE
		
		throw new ExceptionParser("Unknown statement", tokens)
	}
	
	/**
	 * Parse insert statement
	 * @param tokens
	 * @return
	 */
	Map parseInsertStatement(List<Map> tokens) {
		if (lexer == null) throw new ExceptionParser("Required lexer for parsing", tokens)
		
		if (tokens == null || tokens.size() < minSizeInsertStatement) throw new ExceptionParser("Invalid insert DML operator", tokens)
		if (lexer.keyWords(tokens, 0, 2).toUpperCase() != insertKeyWord) throw new ExceptionParser("expected \"$insertKeyWord\"", tokens)
		
		def start = 3
		def object = lexer.object(tokens, 2)
		
		if (object == null || object.isEmpty() || object.size() > 3) throw new ExceptionParser("Expected table name", tokens)
		def tableName = object[object.size() - 1]
		def schemaName = (object.size() > 1)?object[object.size() - 2]:null
		def dbName = (object.size() > 2)?object[object.size() - 3]:null
		
		
		def res = [:]
		res."tableName" = tableName
		if (schemaName != null) res."schemaName" = schemaName
		if (dbName != null) res."dbName" = schemaName
		
		def fields = lexer.list(tokens, start)
		if (fields == null) throw new ExceptionParser("Expected list of field", tokens)
		
		start++
		def valParams = lexer.function(tokens, start)
		if (valParams == null || ((String)valParams."value").toUpperCase() != "VALUES") throw new ExceptionParser("Expected VALUES", tokens)
		
		List<String> values = (List<String>)valParams."list"
		if (fields.size() != values.size()) throw new ExceptionParser("Number of values does not correspond to the number of specified fields", tokens)
		def fv = [:]
		res.put("values", fv)
		for (Integer i = 0; i < fields.size(); i++) {
			fv.put(fields[i]."value", values[i])
		}
		
		start++
		if (start < tokens.size()) {
			if (lexer.type(tokens, start) == Lexer.TokenType.SEMICOLON) start++
		}
		if (start < tokens.size()) {
			println tokens[start]
			throw new ExceptionParser("Expected end statement", tokens)
		}
		
		res
	}
	
	/**
	 * Parse update statement
	 * @param tokens
	 * @return
	 */
	Map parseUpdateStatement(List<Map> tokens) {
		if (lexer == null) throw new ExceptionParser("Required lexer for parsing", tokens)
		
		if (tokens == null || tokens.size() < minSizeUpdateStatement) throw new ExceptionParser("Invalid update DML operator", tokens)
		if (lexer.keyWords(tokens, 0, 1).toUpperCase() != updateKeyWord) throw new ExceptionParser("expected \"$updateKeyWord\"", tokens)
		
		def start = 2
		def object = lexer.object(tokens, 1)
		
		if (object.isEmpty() || object.size() > 3) throw new ExceptionParser("Expected table name", tokens)
		def tableName = object[object.size() - 1]
		def schemaName = (object.size() > 1)?object[object.size() - 2]:null
		def dbName = (object.size() > 2)?object[object.size() - 3]:null
		
		def res = [:]
		res."tableName" = tableName
		if (schemaName != null) res."schemaName" = schemaName
		if (dbName != null) res."dbName" = schemaName
		
		if (lexer.keyWords(tokens, start, 1).toUpperCase() != "SET") throw new ExceptionParser("Expected \"SET\"", tokens)
		start++
		
		def wherePos = lexer.findKeyWord(tokens, start, "WHERE")
		if (wherePos == -1) throw new ExceptionParser("Expected \"WHERE\"", tokens)
		def setFinishPos = wherePos - 1 
		
		Map values = [:]
		res.put("values", values)
		def valueList = lexer.toList(tokens, start, setFinishPos)
		valueList.each { List<Map> setToken ->
			if (setToken.size() != 3) throw new ExceptionParser("Invalid set operator", setToken)
			def token = setToken[1]
			if (token."type" != Lexer.TokenType.OPERATOR || token."value" != "=") throw new ExceptionParser("Invalid field in set operator", setToken)
			token = setToken[0]
			if (!(token."type" == Lexer.TokenType.SINGLE_WORD || (token."type" == Lexer.TokenType.QUOTED_TEXT && token.quote == '"'))) throw new ExceptionParser("Invalid set operator", setToken)
			values.put(token."value", setToken[2])
		}

		Map where = [:]
		res."where" = where		
		def whereList = lexer.toList(tokens, wherePos + 1, tokens.size() - 1, "AND")
		whereList.each { List<Map> whereToken ->
			def token = whereToken[0]
			if (!(token."type" == Lexer.TokenType.SINGLE_WORD || (token."type" == Lexer.TokenType.QUOTED_TEXT && token."quote" == '"'))) throw new ExceptionParser("Invalid where operator", whereToken)
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
		
		res
	}

	Map parseDeleteStatement(List<Map> tokens) {
		if (lexer == null) throw new ExceptionParser("Required lexer for parsing", tokens)
		
		if (tokens == null || tokens.size() < minSizeDeleteStatement) throw new ExceptionParser("Invalid delete DML operator", tokens)
		if (lexer.keyWords(tokens, 0, 2).toUpperCase() != deleteKeyWord) throw new ExceptionParser("expected \"$deleteKeyWord\"", tokens)
		
		def start = 3
		def object = lexer.object(tokens, 2)
		
		if (object.isEmpty() || object.size() > 3) throw new ExceptionParser("Expected table name", tokens)
		def tableName = object[object.size() - 1]
		def schemaName = (object.size() > 1)?object[object.size() - 2]:null
		def dbName = (object.size() > 2)?object[object.size() - 3]:null
		
		def res = [:]
		res."tableName" = tableName
		if (schemaName != null) res."schemaName" = schemaName
		if (dbName != null) res."dbName" = schemaName
		
		if (lexer.keyWords(tokens, start, 1).toUpperCase() != "WHERE") throw new ExceptionParser("Expected \"WHERE\"", tokens)
		start++

		Map where = [:]
		res."where" = where
		def whereList = lexer.toList(tokens, start, tokens.size() - 1, "AND")
		whereList.each { List<Map> whereToken ->
			def token = whereToken[0]
			if (!(token."type" == Lexer.TokenType.SINGLE_WORD || (token."type" == Lexer.TokenType.QUOTED_TEXT && token."quote" == '"'))) throw new ExceptionParser("Invalid where operator", whereToken)
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
		
		res
	}
}
