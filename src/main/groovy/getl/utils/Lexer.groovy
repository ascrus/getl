//file:noinspection unused
package getl.utils

import getl.exception.ExceptionGETL
import groovy.transform.CompileStatic

/**
 * Analyze text as lexer
 * @author Alexsey Konstantinov
 *
 */
@CompileStatic
class Lexer {
	static enum TokenType {SINGLE_WORD, QUOTED_TEXT, LIST, COMMA, SEMICOLON, FUNCTION, OBJECT_NAME, OPERATOR, COMMENT, SINGLE_COMMENT, NUMBER}

	/**
	 * Type of use parse command
	 */
	static private enum CommandType {NONE, WORD, QUOTE, BRACKET, OBJECT_NAME, OPERATOR, COMMENT, SINGLE_COMMENT}

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
		Long first
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
	private Long curPosition

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
			switch (c) {
				case 9: // Tabulation
					curNum += 1
					if (command.type in [CommandType.QUOTE, CommandType.COMMENT, CommandType.SINGLE_COMMENT])
						addChar(c)
					else
						gap(c, true)

					break

				case 32: // Space
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
							input.read()
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
						input.read()
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
						input.read()
						curNum++
						comment_start(false)
						curPosition++
					}
					else if (scriptType == javaScriptType && nextChar == 47) {
						input.read()
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
						gap(c, true)

					break

				default:
					if (!(command.type in [CommandType.QUOTE, CommandType.WORD, CommandType.OBJECT_NAME, CommandType.OPERATOR,
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

	private void addChar(Integer c) {
		sb.append((char)(c as int))
	}

	/**
	 * Gap current word
	 * @param c
	 * @return
	 */
	private Boolean gap(Integer c, Boolean div = false) {
		def res = false
		def lastPos = curPosition + ((div)?-1:0)
		if (command.type in [CommandType.WORD]) {
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
		else if (command.type == CommandType.OBJECT_NAME) {
			if (sb.length() > 0)
				tokens << [type: TokenType.OBJECT_NAME, value: sb.toString(), first: command.first, last: lastPos]

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
			input.read()
            curNum++
			curPosition++
            addChar(n1)
        }

        if (n1 == 42) {
            if (Character.getType(n2) == Character.MATH_SYMBOL || (n2 in [37, 38, 42])) {
				input.read()
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
		if (command.type == CommandType.WORD) {
			if (sb.length() > 0 && sb.substring(sb.length() - 1) == ".") {
				command.type = CommandType.OBJECT_NAME

				return
			}
			else {
				error("unexpectedly found quote in the word")
			}
		}

		if (command.type == CommandType.OBJECT_NAME) {
			return
		}

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
		if (c == n) {
			input.read()
			curNum++
			curPosition++
			addChar(n)
			return
		}
		// If point, change to object name
		else if (n == 46) {
			input.read()
			curNum++
			curPosition++
			addChar(n)
			command.type = CommandType.OBJECT_NAME
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

		if (tokens.size() > 0) {
			def token = tokens[tokens.size() - 1] as Map
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

		Map prevToken = [:]
		if (tokens.size() != 0)
			prevToken = (Map) tokens.get(tokens.size() - 1)

		if (tokens.size() > 0 && prevToken.type == TokenType.FUNCTION && prevToken.list == null && (prevToken?.delimiter as Map)?.type != TokenType.COMMA) {
			prevToken.list = curTokens
			prevToken.start = ((char)(c1 as int)).toString()
			prevToken.finish = ((char)(c2 as int)).toString()
			prevToken.last = curPosition
		}
		else {
			tokens << [type: TokenType.LIST, start: ((char)(c1 as int)).toString(), finish: ((char)(c2 as int)).toString(), list: curTokens, first:
					command.first, last: curPosition]
		}

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
		def token = tokens[tokens.size() - 1]
		token.delimiter = m
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
	List<List<Map>> statements() {
		List<List<Map>> res = []
		def cur = 0
		def pos = FindByType(tokens, TokenType.SEMICOLON, 0)
		while (pos != -1) {
			if (pos >= cur)
				res << tokens.subList(cur, pos)

			cur = pos + 1
			pos = FindByType(tokens, TokenType.SEMICOLON, cur)
		}

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
		StringBuilder sb = new StringBuilder()
		def i = 0
		while (start < tokens.size() && (max == null || i < max)) {
			if ((tokens[start].type as TokenType) in [TokenType.SINGLE_WORD, TokenType.OBJECT_NAME, TokenType.FUNCTION]) {
				i++
				sb << tokens[start].value
				sb << " "
			}
			start++
		}

		return (sb.length() > 0)?sb.substring(0, sb.length() - 1):""
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
		Map token = tokens[position]
		if (token.type != TokenType.FUNCTION)
			return null

		return token
	}

	/**
	 * Return object name for start token while has single or double quoted word delimiters by point
	 * @param position start position in tokens
	 * @return - object elements
	 */
	List object(Integer position) {
		Object(tokens, position)
	}

	/**
	 * Return object name for start token while has single or double quoted word delimiters by point
	 * @param tokens list of tokens
	 * @param position start position in tokens
	 * @return - object elements
	 */
	static List Object(List<Map> tokens, Integer position) {
		if (!((tokens[position].type as TokenType) in [TokenType.SINGLE_WORD, TokenType.OBJECT_NAME])) {
			return null
		}

		return ((String)tokens[position].value).split("[.]").toList()
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
		return (TokenType)((position < tokens.size())?tokens[position].type:null)
	}

	/**
	 * Find token by type
	 * @param type token type
	 * @param start start position in tokens
	 * @return token number
	 */
	Integer findByType(TokenType type, Integer start = 0) {
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
		for (Integer i = start; i < tokens.size(); i++) {
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
		delimiter = delimiter.toUpperCase()
		if (finish == null)
			finish = tokens.size() - 1
		List<List<Map>> res = [] as List<List<Map>>
		def cur = [] as List<Map>
		while (start <= finish && tokens[start].type  != TokenType.SEMICOLON) {
			def token = tokens[start]
			if (((token.type as TokenType) in [TokenType.SINGLE_WORD, TokenType.FUNCTION, TokenType.NUMBER]) &&
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
	 * @param tokens list of tokens
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
		keyWord = keyWord.toUpperCase()
		for (Integer i = start; i < tokens.size(); i++) {
			def token = tokens[i]
			if (token."type" == TokenType.SINGLE_WORD && ((String)token."value").toUpperCase() == keyWord)
				return i
		}

		return -1
	}
}