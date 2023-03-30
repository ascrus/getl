//file:noinspection unused
package getl.utils

import getl.exception.ExceptionGETL
import groovy.transform.CompileStatic
import groovy.transform.NamedVariant

/**
 * Analyze text as lexer
 * @author Alexsey Konstantinov
 *
 */
@CompileStatic
class Lexer {
	static enum TokenType {SINGLE_WORD, QUOTED_TEXT, LIST, COMMA, SEMICOLON, FUNCTION, OPERATOR, COMMENT, SINGLE_COMMENT, NUMBER, LINE_FEED}

	/**
	 * Type of use parse command
	 */
	static private enum CommandType {NONE, WORD, QUOTE, BRACKET, OPERATOR, COMMENT, SINGLE_COMMENT}

	/** Type language in the script */
	enum ScriptType {JAVA, SQL}
	/** Java script */
	public static ScriptType javaScriptType = ScriptType.JAVA
	/** SQL script */
	public static ScriptType sqlScriptType = ScriptType.SQL

	/** Create new lexer */
	Lexer() { }

	/** 
	 * Create new lexer
	 * @param reader input reader
	 */
	Lexer(Reader reader) {
		this.input = reader
	}

	/**
	 * Create new lexer
	 * @param text input text
	 * @param typeComments use specified type comments
	 */
	Lexer(String text, ScriptType typeComments = null) {
		this.input = new StringReader(text)
		this.scriptType = typeComments
		try {
			parse()
		}
		finally {
			input.close()
		}
	}

	/** Comments in the script */
	private ScriptType scriptType
	/** Comments in the script */
	ScriptType getScriptType() { scriptType }
	/** Comments in the script */
	void setScriptType(ScriptType value) { scriptType }

	/** Input text stream */
	private Reader input
	/** Input text stream */
	Reader getInput() { input }
	/** Input text stream */
	void setInput(Reader value) {
		if (!value.ready())
			throw new ExceptionGETL("Input reader not ready!")

		input = value
	}

	/** Result tokens */
	private List<Map> tokens
	/** Result tokens */
	List<Map> getTokens() { tokens }

	/** Parsed script */
	String getScript() { script }

	/**
	 * Parameter of command
	 * @author Alexsey Konstantinov
	 *
	 */
	class CommandParam {
		/** Command type */
		CommandType type
		/** Value in operator */
		Integer value
		/** Start character*/
		Integer start
		/** Finish character */
		Integer finish
		/** First position in script */
		Integer first
		/** Text buffer */
		StringBuilder sb
	}

	/** Current parse command */
	private CommandParam command

	/**	 Stack running of commands */
	private Stack commands

	/** Current text buffer */
	private StringBuilder sb

	/** Script text buffer */
	private StringBuilder scriptBuf

	/** Parsed script */
	private String script

	/** Stack tokens by lists */
	private Stack stackTokens

	/** Current column number in line */
	private Integer curNum

	/** Current line */
	private Integer curLine

	/** Current position in parsed script */
	private Integer curPosition

	/** Parse input stream */
	void parse() {
		tokens = []
		command = new CommandParam()
		command.type = CommandType.NONE
		commands = new Stack()
		stackTokens = new Stack()
		sb = new StringBuilder()
		scriptBuf = new StringBuilder()

		curNum = 0
		curLine = 1
		curPosition = -1

		def c = input.read()
		while (c != -1) {
			curNum++
			curPosition++
			appendToScript(c)
			//noinspection GroovyFallthrough
			switch (c) {
				case 92: // Escape char \
					if (scriptType == javaScriptType && command.type == CommandType.QUOTE) {
						c = input.read()
						if (c == -1)
							error('No character for escape sequence')

						curNum++
						curPosition++
						addChar(c)
						break
					}

				case 32: // Space
				case 9: // Tabulation
					curNum += 1
					if (command.type in [CommandType.QUOTE, CommandType.COMMENT, CommandType.SINGLE_COMMENT])
						addChar(c)
					else
						gap(c, true)

					break

                case { Character.getType(c) == Character.MATH_SYMBOL }: // for operator chars, such as: +, -, >, <, = or single sql comment --
                case [33, 37, 38, 45]: // for !, %, &, - chars
                    if (command.type in [CommandType.QUOTE, CommandType.COMMENT, CommandType.SINGLE_COMMENT])
						addChar(c)
                    else if (scriptType == sqlScriptType && c == 45) {
						input.mark(1)
						def nextChar = input.read()
						input.reset()

						if (nextChar == 45) {
							appendToScript(input.read())
							curNum++
							comment_start(true)
							curPosition++
						}
						else {
							gap(c, true)
							operator(c)
						}
					}
					else {
                        gap(c, true)
                        operator(c)
                    }

                    break

				case 42: // finish comment */ or multiplication *
					input.mark(1)
					def nextChar = input.read()
					input.reset()

					if (command.type in [CommandType.QUOTE, CommandType.COMMENT, CommandType.SINGLE_COMMENT] && nextChar != 47) {
						addChar(c)
					} else if (command.type == CommandType.COMMENT && nextChar == 47) {
						appendToScript(input.read())
						curNum++
						curPosition++
						comment_finish()
					} else if (scriptType == sqlScriptType && command.type == CommandType.WORD &&
							sb.length() > 0 && sb.charAt(sb.length() - 1) == (char)'.') {
						addChar(c)
					}
					else {
						gap(c)
						operator(c)
					}

					break

				case 39: case 34: // Single and double quote <'">
					quote(c)
					break

				case 40: // opening bracket <(>
					level_start(c, 41)
					break

				case 91: // opening bracket <[>
					level_start(c, 93)
					break

				case 123: // opening bracket <{>
					level_start(c, 125)
					break

				case 41: // closing bracket <)>
					level_finish(40, c)
					break

				case 93: // closing bracket <]>
					level_finish(91, c)
					break

				case 125: // closing bracket <}>
					level_finish(123, c)
					break

				case 47: // start comment /* or single comment // or dividing /
					input.mark(1)
					def nextChar = input.read()
					input.reset()

					if (command.type in [CommandType.QUOTE, CommandType.COMMENT, CommandType.SINGLE_COMMENT]) {
						addChar(c)
					} else if (nextChar == 42 && scriptType != null) {
						appendToScript(input.read())
						curNum++
						comment_start(false)
						curPosition++
					}
					else if (scriptType == javaScriptType && nextChar == 47) {
						appendToScript(input.read())
						curNum++
						comment_start(true)
						curPosition++
					}
					else {
						gap(c)
						operator(c)
					}

					break

				case 44: // comma (,)
					comma(c)
					break

				case 59: // semicolon (;)
					semicolon(c)
					break

				case 13: // carriage return
					curNum--
					break

				case 10: // line feed
					curNum = 0
					curLine++
					if (command.type == CommandType.SINGLE_COMMENT) {
						comment_finish()
					}
					else if (command.type in [CommandType.QUOTE, CommandType.COMMENT, CommandType.SINGLE_COMMENT])
						addChar(c)
					else
						if (gap(c, true) || command.type == CommandType.NONE) {
							tokens << [type: TokenType.LINE_FEED, value: '\n', first: curPosition, last: curPosition]
						}

					break

				default:
					if (!(command.type in [CommandType.QUOTE, CommandType.WORD, CommandType.OPERATOR,
										   CommandType.COMMENT, CommandType.SINGLE_COMMENT])) {
						commands.push(command)
						command = new CommandParam()
						command.type = CommandType.WORD
						command.first = curPosition
					}
					addChar(c)
			}

			c = input.read()
		}

        if (sb.length() > 0)
			gap(c)

		script = scriptBuf.toString()
		sb = null
		scriptBuf = null
	}

	/** Adding character to current text buffer */
	private void addChar(Integer c) {
		sb.append((char)(c as int))
	}

	/** Adding character to script text buffer */
	private void appendToScript(Integer c) {
		scriptBuf.append((char)(c as int))
	}

	/**
	 * Gap current word
	 * @param c
	 * @return
	 */
	private Boolean gap(Integer c, Boolean div = false) {
		def res = false
		def lastPos = curPosition + ((div)?-1:0)
		if (command.type == CommandType.WORD) {
			if (sb.length() > 0) {
				def str = sb.toString()
				if (str.isNumber()) {
					def val = null
					if (str.isInteger())
						val = str.toInteger()
					else if (str.isLong())
						val = str.toLong()
					else if (str.isBigDecimal())
						val = str.toBigDecimal()
					else if (str.isFloat())
						val = str.toFloat()
					else if (str.isDouble())
						val = str.toDouble()
					else
						error("unknown number \"$str\"")
					tokens << [type: TokenType.NUMBER, value: val, first: command.first, last: lastPos]
				}
				else
					tokens << [type: TokenType.SINGLE_WORD, value: str, first: command.first, last: lastPos]
			}

			command = commands.pop() as CommandParam
			sb = new StringBuilder()

			res = true
		}
		else if (command.type in [CommandType.SINGLE_COMMENT, CommandType.COMMENT]) {
			comment_finish()
			res = true
		}

		return res
	}

    /**
     * Check operator chars
     * @param c
     * @param type
     */
    private void operator(Integer c) {
        if (command.type != CommandType.OPERATOR) {
            commands.push(command)
            command = new CommandParam()
            command.type = CommandType.OPERATOR
			command.first = curPosition
            command.value = c
            addChar(c)
        }

        input.mark(2)
        def n1 = input.read()
		def n2 = input.read()
		input.reset()
        if (Character.getType(n1) == Character.MATH_SYMBOL || (n1 in [37, 38, 42])) {
			appendToScript(input.read())
            curNum++
			curPosition++
            addChar(n1)
        }

        if (n1 == 42) {
            if (Character.getType(n2) == Character.MATH_SYMBOL || (n2 in [37, 38, 42])) {
				appendToScript(input.read())
                curNum++
				curPosition++
                addChar(n2)
            }
        }

        if (sb.length() > 0)
			tokens << [type: TokenType.OPERATOR, value: sb.toString(), first: command.first, last: curPosition]

        command = commands.pop() as CommandParam
        sb = new StringBuilder()
    }

	/**
	 * Valid quoted text
	 * @param c
	 * @param type
	 */
	private void quote(Integer c) {
		if (command.type in [CommandType.COMMENT, CommandType.SINGLE_COMMENT]) {
			addChar(c)
			return
		}

		if (command.type == CommandType.WORD)
			gap(c, true)

		if (command.type != CommandType.QUOTE) {
			commands.push(command)
			command = new CommandParam()
			command.type = CommandType.QUOTE
			command.first = curPosition
			command.value = c
			return
		}

		if (c != command.value) {
			addChar(c)
			return
		}

		input.mark(1)
		def n = input.read()
		input.reset()
		if (scriptType == sqlScriptType && c == n) {
			appendToScript(input.read())
			curNum++
			curPosition++
			addChar(n)
			return
		}

		if (sb.length() >= 0)
			tokens << [type: TokenType.QUOTED_TEXT, quote: ((char)(c as int)).toString(), value: sb.toString(), first: command.first, last: curPosition]

		command = commands.pop() as CommandParam
		sb = new StringBuilder()
	}

	/**
	 * Start comments block
	 */
	private void comment_start(/*Integer c, Integer nextChar, */Boolean singleComment) {
		commands.push(command)
		command = new CommandParam()
		command.type = (singleComment)?CommandType.SINGLE_COMMENT:CommandType.COMMENT
		command.first = curPosition
		if (!singleComment && sb.length() > 0) {
			command.sb = sb
			sb = new StringBuilder()
		}
	}

	/** Finish comments block */
	private void comment_finish() {
		if (sb.length() >= 0) {
			if (command.type == CommandType.COMMENT)
				tokens << [type: TokenType.COMMENT, comment_start: "/*", comment_finish: "*/", value: sb.toString(), first: command.first, last: curPosition]
			else if (scriptType == javaScriptType)
				tokens << [type: TokenType.SINGLE_COMMENT, comment_start: "//", comment_finish: '\n', value: sb.toString(), first: command.first, last: curPosition]
			else if (scriptType == sqlScriptType)
				tokens << [type: TokenType.SINGLE_COMMENT, comment_start: "--", comment_finish: '\n', value: sb.toString(), first: command.first, last: curPosition]
			else
				throw new ExceptionGETL("Unknown comment type \"$scriptType\"!")
		}
		if (command.sb != null)
			sb = command.sb
		else
			sb = new StringBuilder()

		command = commands.pop() as CommandParam
	}

	/**
	 * Start bracket level
	 * @param c
	 * @param type
	 */
	private void level_start (Integer c1, Integer c2) {
		if (command.type in [CommandType.QUOTE, CommandType.COMMENT, CommandType.SINGLE_COMMENT]) {
			addChar(c1)
			return
		}

		gap(c1, true)

		def size = tokens.size()
		if (size > 0) {
			def token = tokens[size - 1] as Map
			def tokenDelimiterType = token.delimiter as Map
			if (token.type == TokenType.SINGLE_WORD && tokenDelimiterType?.type != TokenType.COMMA) {
				token.type = TokenType.FUNCTION
			}
		}

		commands.push(command)
		command = new CommandParam()
		command.type = CommandType.BRACKET
		command.first = curPosition
		command.start = c1
		command.finish = c2

		stackTokens.push(tokens)
		tokens = [] as List<Map>
	}

	private void level_finish (Integer c1, Integer c2) {
		if (command.type in [CommandType.QUOTE, CommandType.COMMENT, CommandType.SINGLE_COMMENT]) {
			addChar(c2)
			return
		}

		gap(c2, true)

		if (command.type != CommandType.BRACKET || command.start != c1 || command.finish != c2)
			error("opening bracket not found")

		List curTokens = tokens
		tokens = stackTokens.pop() as List

		Map prevToken = new HashMap()
		def size = tokens.size()
		if (size != 0)
			prevToken = (Map) tokens.get(size - 1)

		if (size > 0 && prevToken.type == TokenType.FUNCTION && prevToken.list == null && (prevToken?.delimiter as Map)?.type != TokenType.COMMA) {
			prevToken.list = curTokens
			prevToken.start = ((char)(c1 as int)).toString()
			prevToken.finish = ((char)(c2 as int)).toString()
			prevToken.last = curPosition
		}
		else
			tokens << [type: TokenType.LIST, start: ((char)(c1 as int)).toString(), finish: ((char)(c2 as int)).toString(), list: curTokens, first:
					command.first, last: curPosition]

		command = commands.pop() as CommandParam
	}

	private void comma(Integer c) {
		if (command.type in [CommandType.QUOTE, CommandType.COMMENT, CommandType.SINGLE_COMMENT]) {
			addChar(c)
			return
		}

		if (!gap(c, true) && !(command.type in [CommandType.NONE, CommandType.BRACKET]))
			error("unexpected comma")

		Map m = [type: TokenType.COMMA, value: ((char)(c as int)).toString()]

		if (!tokens.isEmpty()) {
			def token = tokens[tokens.size() - 1]
			token.delimiter = m
		}
	}

	private void semicolon(Integer c) {
		if (command.type in [CommandType.QUOTE, CommandType.COMMENT, CommandType.SINGLE_COMMENT]) {
			addChar(c)
			return
		}

		gap(c, true)

		tokens << [type: TokenType.SEMICOLON, value: ((char)(c as int)).toString(), first: curPosition, last: curPosition]
	}

	protected error(String message) {
		throw new ExceptionGETL("Syntax error line $curLine, col $curNum: $message, current command: type=${command.type};value=${command.value};" +
				"start=${command.start};finish=${command.finish};buffer=${sb.toString()}")
	}

	@Override
	String toString() {
		MapUtils.ToJson([tokens: tokens])
	}

	/**
	 * Return list of statements separated by a semicolon
	 * @return
	 */
	List<List<Map>> statements(TokenType statementType = TokenType.SEMICOLON) {
		if (tokens == null)
			return null
		if (statementType == null)
			throw new ExceptionGETL('Required statement type!')

		List<List<Map>> res = []
		def cur = 0
		def pos = FindByType(tokens, statementType, 0)
		while (pos != -1) {
			if (pos >= cur) {
				def list = tokens.subList(cur, pos)
				def listSize = list.size()
				if (listSize > 0) {
					def i = -1
					while (i < listSize && list[i + 1].type == TokenType.LINE_FEED)
						i++

					if (i != -1) {
						if (i < listSize - 1)
							res.add(list.subList(i + 1, listSize))
					} else
						res.add(list)
				}
			}

			cur = pos + 1
			pos = FindByType(tokens, statementType, cur)
		}

		def tokenSize = tokens.size()
		while (cur < tokenSize && tokens[cur].type == TokenType.LINE_FEED)
			cur++

		if (cur < tokenSize)
			res.add(tokens.subList(cur, tokenSize))

		return res
	}

	/**
	 * Return key words for start token while words is single word
	 * @param start start position by token (default 1)
	 * @param max maximum number (null for all)
	 * @return word string
	 */
	String keyWords(Integer start = 0, Integer max = null) {
		KeyWords(tokens, start, max)
	}

	/**
	 * Return key words for start token while words is single word
	 * @param tokens list of tokens
	 * @param start start position by token
	 * @param max maximum number (null for all)
	 * @return word string
	 */
	static String KeyWords(List<Map> tokens, Integer start = 0, Integer max = null) {
		if (tokens == null)
			return null

		StringBuilder sb = new StringBuilder()
		def i = 0
		def size = tokens.size()
		while (start < size && (max == null || i < max)) {
			if ((tokens[start].type as TokenType) in [TokenType.SINGLE_WORD, TokenType.FUNCTION]) {
				i++
				sb << tokens[start].value
				sb << " "
			}
			start++
		}

		return (sb.length() > 0)?sb.substring(0, sb.length() - 1):""
	}

	/**
	 * Return script for specified tokens
	 * @param start start position by token
	 * @param finish finish position by token (null for all)
	 * @param ignoreComments ignore comments
	 * @return script
	 */
	@NamedVariant
	String scriptBuild(List<Map> tokens = this.tokens, Integer start = 0, Integer finish = null, Boolean ignoreComments = false) {
		if (tokens == null)
			tokens = this.tokens

		if (tokens.isEmpty())
			return ''

		if (start == null)
			start = 0

		if (ignoreComments == null)
			ignoreComments = false

		StringBuilder sb = new StringBuilder()
		def cur = tokens[start].first as Integer
		def tokenSize = tokens.size()
		def last = -1
		while (start < tokenSize && (finish == null || start <= finish)) {
			def token = tokens[start]
			if (ignoreComments && (token.type as TokenType) in [TokenType.COMMENT, TokenType.SINGLE_COMMENT]) {
				def commentStart = token.first as Integer
				if (commentStart > cur)
					sb.append(script.substring(cur, commentStart))

				cur = (token.last as Integer) + 1
			}
			else
				last = token.last as Integer

			start++
		}
		if (cur <= last)
			sb.append(script.substring(cur, last + 1))

		return sb.toString()
	}

	/**
	 * Return list for position token
	 * @param position position in tokens
	 * @return list elements
	 */
	List<Map> list(Integer position) {
		List(tokens, position)
	}

	/**
	 * Return list for position token
	 * @param tokens list of tokens
	 * @param position position in tokens
	 * @return list elements
	 */
	static List<Map> List(List<Map> tokens, Integer position) {
		if (tokens == null)
			return null

		return (List<Map>)((tokens[position].type == TokenType.LIST)?tokens[position].list:null)
	}

	/**
	 * Return function name and parameters for position token
	 * @param position position in tokens
	 * @return list elements
	 */
	Map function(Integer position) {
		Function(tokens, position)
	}

	/**
	 * Return function name and parameters for position token
	 * @param tokens list of tokens
	 * @param position position in tokens
	 * @return list elements
	 */
	static Map Function(List<Map> tokens, Integer position) {
		if (tokens == null)
			return null

		Map token = tokens[position]
		if (token.type != TokenType.FUNCTION)
			return null

		return token
	}

	/**
	 * Return token type
	 * @param position position in tokens
	 * @return token type
	 */
	TokenType type(Integer position) {
		Type(tokens, position)
	}

	/**
	 * Return token type
	 * @param tokens list of tokens
	 * @param position position in tokens
	 * @return token type
	 */
	static TokenType Type(List<Map> tokens, Integer position) {
		if (tokens == null)
			return null

		return (TokenType)((position < tokens.size())?tokens[position].type:null)
	}

	/**
	 * Find token by type
	 * @param type token type
	 * @param start start position in tokens
	 * @return token number
	 */
	Integer findByType(TokenType type, Integer start = 0) {
		if (tokens == null)
			return null

		FindByType(tokens, type, start)
	}

	/**
	 * Find token by type
	 * @param tokens list of tokens
	 * @param type token type
	 * @param start start position in tokens
	 * @return token number
	 */
	static Integer FindByType(List<Map> tokens, TokenType type, Integer start = 0) {
		if (tokens == null)
			return null

		def size = tokens.size()
		for (Integer i = start; i < size; i++) {
			if (tokens[i].type == type)
				return i
		}

		return -1
	}

	/**
	 * Convert tokens with comma separate to list
	 * @param tokens list of tokens
	 * @param start start position in tokens
	 * @param finish finish position in tokens
	 * @param delimiter list delimiter
	 * @return list elements
	 */
	List<List<Map>> toList(Integer start = 0, Integer finish = null, String delimiter = ',') {
		ToList(tokens, start, finish, delimiter)
	}

	/**
	 * Convert tokens with comma separate to list
	 * @param tokens list of tokens
	 * @param start start position in tokens
	 * @param finish finish position in tokens
	 * @param delimiter list delimiter
	 * @return list elements
	 */
	static List<List<Map>> ToList(List<Map> tokens, Integer start = 0, Integer finish = null, String delimiter = ',') {
		if (tokens == null)
			return null

		delimiter = delimiter.toUpperCase()
		if (finish == null)
			finish = tokens.size() - 1
		List<List<Map>> res = [] as List<List<Map>>
		def cur = [] as List<Map>
		while (start <= finish && tokens[start].type  != TokenType.SEMICOLON) {
			def token = tokens[start]
			if (((token.type as TokenType) in [TokenType.SINGLE_WORD, TokenType.FUNCTION, TokenType.NUMBER, TokenType.QUOTED_TEXT]) &&
					((token.value as String).toUpperCase() == delimiter || (token.delimiter as Map)?.value == delimiter)) {
				if (token.delimiter != null)
					cur << token
				res << cur
				cur = [] as List<Map>
			}
			else {
				cur << token
			}

			start++
		}
		if (!cur.isEmpty())
			res << cur

		return res
	}

	/**
	 * Return position key word by start position
	 * @param start start position in tokens
	 * @param keyWord key word
	 * @return position number
	 */
	Integer findKeyWord(String keyWord, Integer start = 0) {
		FindKeyWord(tokens, keyWord, start)
	}

	/**
	 * Return position key word by start position
	 * @param tokens list of tokens
	 * @param start start position in tokens
	 * @param keyWord key word
	 * @return position number
	 */
	static Integer FindKeyWord(List<Map> tokens, String keyWord, Integer start = 0) {
		FindKeyWithType(tokens, [TokenType.SINGLE_WORD], keyWord, start)
	}

	/**
	 * Return position function by start position
	 * @param start start position in tokens
	 * @param keyWord function name
	 * @return position number
	 */
	Integer findFunction(String funcName, Integer start = 0) {
		FindFunction(tokens, funcName, start)
	}

	/**
	 * Return position function by start position
	 * @param tokens list of tokens
	 * @param start start position in tokens
	 * @param keyWord function name
	 * @return position number
	 */
	static Integer FindFunction(List<Map> tokens, String funcName, Integer start = 0) {
		FindKeyWithType(tokens, [TokenType.FUNCTION], funcName, start)
	}

	/**
	 * Return position list by start position
	 * @param start start position in tokens
	 * @return position number
	 */
	Integer findList(Integer start = 0) {
		FindList(tokens, start)
	}

	/**
	 * Return position function by start position
	 * @param tokens list of tokens
	 * @param start start position in tokens
	 * @return position number
	 */
	static Integer FindList(List<Map> tokens, Integer start = 0) {
		FindKeyWithType(tokens, [TokenType.LIST], null, start)
	}

	/**
	 * Return position key word by start position
	 * @param types list of token type
	 * @param key key word
	 * @param start start position in tokens
	 * @return position number
	 */
	Integer findKeyWithType(List<TokenType> types, String key, Integer start = 0) {
		FindKeyWithType(tokens, types, key, start)
	}

	/**
	 * Return position key word by start position
	 * @param tokens list of tokens
	 * @param types list of token type
	 * @param key key word
	 * @param start start position in tokens
	 * @return position number
	 */
	static Integer FindKeyWithType(List<Map> tokens, List<TokenType> types, String key, Integer start = 0) {
		if (tokens == null)
			return null

		key = key.toUpperCase()
		def size = tokens.size()
		for (Integer i = start; i < size; i++) {
			def token = tokens[i]
			if ((token.type as TokenType) in types && (key == null || (token.value as String).toUpperCase() == key))
				return i
		}

		return -1
	}
}