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
		SELECT, INSERT, UPDATE, DELETE, MERGE, TRUNCATE, CREATE, ALTER, DROP, COMMIT, ROLLBACK, START_TRANSACTION,
		GETL_ECHO, GETL_IF, GETL_SET, GETL_FOR, GETL_COMMAND, GETL_EXIT,
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

	@SuppressWarnings('SpellCheckingInspection')
	private Map<String, String> regexp = [
			GETL_ECHO: '(?i)[@]{0,1}ECHO .+',
			GETL_IF: '(?i)[@]{0,1}IF DO',
			GETL_SET: '(?i)[@]{0,1}SET .+',
			GETL_FOR: '(?i)[@]{0,1}FOR DO',
			GETL_COMMAND: '(?i)[@]{0,1}COMMAND',
			GETL_EXIT: '(?i)[@]{0,1}EXIT',
			GETL_ERROR: '(?i)[@]{0,1}ERROR .+',
			GETL_LOAD_POINT: '(?i)[@]{0,1}LOAD[_]POINT .+ TO .+ WITH (INSERT|MERGE)',
			GETL_SAVE_POINT: '(?i)[@]{0,1}SAVE[_]POINT .+ FROM .+ WITH (INSERT|MERGE)',
	        INSERT: '(?i)INSERT INTO\\s+.+\\s+.+',
			UPDATE: '(?i)UPDATE .+\\s+SET\\s+.+',
			DELETE: '(?i)DELETE FROM.+\\s+WHERE\\s+.+',
			MERGE: '(?i)MERGE INTO\\s+.+',
			TRUNCATE: '(?i)TRUNCATE TABLE\\s+.+',
			START_TRANSACTION: '(?i)^(START\\s+|BEGIN\\s+)?TRAN(SACTION)?$',
			COMMIT: 'COMMIT',
			ROLLBACK: 'ROLLBACK',
			CREATE: '(?i)CREATE (\\w+[ ]){0,2}(TABLE|VIEW|PROCEDURE|FUNCTION|SCHEMA|PROJECTION|INDEX) .+',
			ALTER: '(?i)ALTER (TABLE|VIEW|PROCEDURE|FUNCTION|SCHEMA|PROJECTION|INDEX) .+',
			DROP: '(?i)DROP (TABLE|VIEW|PROCEDURE|FUNCTION|SCHEMA|PROJECTION|INDEX) .+',
			SELECT: '(?i)(WITH)?.*SELECT(?!(.+ INTO ){1}).* FROM .+'
	]

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
	 * Generate list of sql script for lexer tokens
	 * @return sql scripts
	 */
	List<String> scripts() {
		if (lexer == null)
			throw new ExceptionParser('Required lexer for parsing!', null)

		if (lexer.tokens == null)
			throw new ExceptionParser('Lexer not parsing!', null)

		def res = [] as List<String>
		def addToRes = { String text ->
			if (text.trim().length() > 0)
				res.add(text)
		}

		def stats = lexer.statements()
		def countStats = stats.size()
		for (int curStat = 0; curStat < countStats; curStat++) {
			def tokens = stats[curStat] as List<Map>
			def curPos = tokens[0].first as Integer

			def isFirstOperator = true
			for (int i = 0; i < tokens.size(); i++) {
				def token = tokens[i] as Map
				def type = token.type as Lexer.TokenType
				def value = token.value as String
				if (isFirstOperator && type in [Lexer.TokenType.SINGLE_WORD, Lexer.TokenType.FUNCTION]) {
					if (value.toUpperCase() in ['ECHO', '@ECHO']) {
						def newPos = token.first as Integer
						if (newPos > curPos)
							addToRes.call(lexer.script.substring(curPos, newPos))

						i = lexer.FindByType(tokens, Lexer.TokenType.LINE_FEED, i)
						if (i == -1)
							i = tokens.size() - 1

						token = tokens[i]
						def lastPos = (token.last as Integer)
						res.add(lexer.script.substring(newPos, lastPos + 1).trim())
						curPos = lastPos + 1
						isFirstOperator = true
						type = Lexer.TokenType.LINE_FEED
					}
					else if (value.toUpperCase() in ['IF', '@IF', 'FOR', '@FOR']) {
						def newPos = token.first as Integer
						if (newPos > curPos) {
							def x = i - 1
							while (x >= 0) {
								if ((tokens[x].type as Lexer.TokenType) in [Lexer.TokenType.SINGLE_COMMENT, Lexer.TokenType.COMMENT])
									newPos = tokens[x].first as Integer
								else if ((tokens[x].type as Lexer.TokenType) != Lexer.TokenType.LINE_FEED)
									break

								x--
							}
							if (newPos > curPos)
								addToRes.call(lexer.script.substring(curPos, newPos))
						}

						i = lexer.FindFunction(tokens, 'DO', i)
						if (i == -1 || tokens[i].type != Lexer.TokenType.FUNCTION)
							throw new ExceptionParser("For the \"IF\" statement at position $newPos, the  \"DO\" block was not found!", tokens)

						token = tokens[i]
						def lastPos = (token.last as Integer)
						res.add(lexer.script.substring(newPos, lastPos + 1).trim())
						curPos = lastPos + 1
					}
					else if (type == Lexer.TokenType.FUNCTION && value.toUpperCase() in ['COMMAND', '@COMMAND']) {
						def newPos = token.first as Integer
						if (newPos > curPos) {
							def x = i - 1
							while (x >= 0) {
								if ((tokens[x].type as Lexer.TokenType) in [Lexer.TokenType.SINGLE_COMMENT, Lexer.TokenType.COMMENT])
									newPos = tokens[x].first as Integer
								else if ((tokens[x].type as Lexer.TokenType) != Lexer.TokenType.LINE_FEED)
									break

								x--
							}
							if (newPos > curPos)
								addToRes.call(lexer.script.substring(curPos, newPos))
						}

						def lastPos = (token.last as Integer)
						res.add(lexer.script.substring(newPos, lastPos + 1).trim())
						curPos = lastPos + 1
					}
				}
				if (!(type in [Lexer.TokenType.COMMENT, Lexer.TokenType.SINGLE_COMMENT, Lexer.TokenType.LINE_FEED]))
					isFirstOperator = false
			}

			if (curPos < (tokens[tokens.size() - 1].first as Integer)) {
				def lastPos = (tokens[tokens.size() - 1].last as Integer)
				addToRes.call(lexer.script.substring(curPos, lastPos + 1))
			}
		}

		return res.collect { it.trim() }
	}
}