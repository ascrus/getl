package getl.utils

/**
 GETL - based package in Groovy, which automates the work of loading and transforming data. His name is an acronym for «Groovy ETL».
 
  GETL is a set of libraries of pre-built classes and objects that can be used to solve problems unpacking,
  transform and load data into programs written in Groovy, or Java, as well as from any software that supports
  the work with Java classes.
  
  Copyright (C) 2013-2014  Alexsey Konstantonov (ASCRUS)
 
  This program is free software: you can redistribute it and/or modify
  it under the terms of the GNU Lesser General Public License as published by
  the Free Software Foundation, either version 3 of the License, or
  (at your option) any later version.
 
  This program is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  GNU General Public License for more details.
 
  You should have received a copy of the GNU General Public License and
  GNU Lesser General Public License along with this program.
  If not, see <http://www.gnu.org/licenses/>.
 */

import getl.exception.ExceptionParser

/**
 * SQL parser
 * @author Alexsey Konstantinov
 *
 */
@groovy.transform.CompileStatic
class SQLParser {
	/**
	 * List of type statements
	 */
	public static enum StatementType {INSERT, UPDATE, DELETE}
	
	/**
	 * Lexer for parsing
	 */
	Lexer lexer
	
	private static final int minSizeInsertStatement = 5
	private static final String insertKeyWord = "INSERT INTO"
	
	private static final int minSizeUpdateStatement = 8
	private static final String updateKeyWord = "UPDATE"
	
	private static final int minSizeDeleteStatement = 6
	private static final String deleteKeyWord = "DELETE FROM"
	
	public StatementType statementType(List tokens) {
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
	public Map parseInsertStatement(List<Map> tokens) {
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
		for (int i = 0; i < fields.size(); i++) {
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
	public Map parseUpdateStatement(List<Map> tokens) {
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
			if (token."type" != Lexer.TokenType.SINGLE_WORD || token."value" != "=") throw new ExceptionParser("Invalid field in set operator", setToken)
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
			if (token."type" == Lexer.TokenType.SINGLE_WORD && token."value" == "=") {
				value = whereToken.subList(2, whereToken.size())
			} 
			else {
				value = whereToken.subList(1, whereToken.size())
			}
			
			where.put(fieldName, value)
		}
		
		res
	}
	
	public Map parseDeleteStatement(List<Map> tokens) {
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
			if (token."type" == Lexer.TokenType.SINGLE_WORD && token."value" == "=") {
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
