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
	static enum TokenType {SINGLE_WORD, QUOTED_TEXT, LIST, COMMA, SEMICOLON, FUNCTION, OBJECT_NAME, OPERATOR, COMMENT}

	/**
	 * Type of use parse command
	 */
	static private enum CommandType {NONE, WORD, QUOTE, BRACKET, OBJECT_NAME, OPERATOR, COMMENT}

	/**
	 * Input text stream
	 */
	public Reader input

	/**
	 * Result tokens
	 */
	public List<Map> tokens

	/**
	 * Parameter of command
	 * @author Alexsey Konstantinov
	 *
	 */
	class CommandParam {
		CommandType type
		Integer value
		Integer start
		Integer finish
	}

	/**
	 * Current parse command
	 */
	private CommandParam command

	/**
	 * Stack running of commands
	 */
	private Stack commands

	/**
	 * Current text
	 */
	private StringBuilder sb

	/**
	 * Stack tokens by lists
	 */
	private Stack stackTokens

	/**
	 * Current column number in line
	 */
	private Integer curNum

	/**
	 * Current line
	 */
	private Integer curLine

	/**
	 * Parse input stream
	 */
	void parse () {
		tokens = []
		command = new CommandParam()
		command.type = CommandType.NONE
		commands = new Stack()
		stackTokens = new Stack()
		sb = new StringBuilder()

		curNum = 0
		curLine = 1

		def c = input.read()
		while (c != -1) {
			curNum++
			switch (c) {
				// Tab
				case 9:
					curNum += 3
					if (command.type in [CommandType.QUOTE, CommandType.COMMENT]) addChar(c) else gap(c)
					break
				// Space
				case 32:
					if (command.type in [CommandType.QUOTE, CommandType.COMMENT]) addChar(c) else gap(c)
					break
                case { Character.getType(c) == Character.MATH_SYMBOL }: // for operator chars, such as: +, -, >, <, =
                case [33, 37, 38, 45]: // for !, %, &, - chars
                    if (command.type in [CommandType.QUOTE, CommandType.COMMENT]) addChar(c)
                    else {
                        gap(c)
                        operator(c)
                    }
                    break
				// special rules for (*) operator
				case 42:
					input.mark(1)
					def nextChar = input.read()

					if (command.type in [CommandType.QUOTE, CommandType.COMMENT] && nextChar != 47) {
						input.reset()
						addChar(c)
					} else if (command.type == CommandType.COMMENT && nextChar == 47) {
						comment_finish(nextChar)
					} else {
						input.reset()
						gap(c)
						operator(c)
					}
					break
				// Single and double quote <'">
				case 39: case 34:
					quote(c)
					break
				// opening bracket <(>
				case 40:
					level_start(c, 41)
					break
				// opening bracket <[>
				case 91:
					level_start(c, 93)
					break
				// opening bracket <{>
				case 123:
					level_start(c, 125)
					break
				// closing bracket <)>
				case 41:
					level_finish(40, c)
					break
				// closing bracket <]>
				case 93:
					level_finish(91, c)
					break
				// closing bracket <}>
				case 125:
					level_finish(123, c)
					break
				// (/) comment character
				case 47:
					input.mark(1)
					def nextChar = input.read()

					if (command.type in [CommandType.QUOTE, CommandType.COMMENT]) {
						input.reset()
						addChar(c)
					} else if (nextChar == 42) {
						input.reset()
						comment_start(c)
					} else {
						input.reset()
						gap(c)
						operator(c)
					}
					break
				// comma (,)
				case 44:
					comma(c)
					break
				// semicolon (;)
				case 59:
					semicolon(c)
					break
				// carriage return
				case 13:
					curNum--
					break
				// line feed
				case 10:
					curNum = 0
					curLine++
					if (command.type in [CommandType.QUOTE, CommandType.COMMENT])
						addChar(c)
					else
						gap(c)
					break
				default:
					if (!(command.type in [CommandType.QUOTE, CommandType.WORD, CommandType.OBJECT_NAME, CommandType.OPERATOR, CommandType.COMMENT])) {
						commands.push(command)
						command = new CommandParam()
						command.type = CommandType.WORD
					}
					addChar(c)
			}

			c = input.read()
		}

        if (sb.length() > 0) gap(c)
	}

	private void addChar(Integer c) {
		sb << (char)(c as int)
	}

	/**
	 * Gap current word
	 * @param c
	 * @return
	 */
	private Boolean gap (Integer c) {
		if (command.type in [CommandType.WORD]) {
			if (sb.length() > 0) tokens << [type: TokenType.SINGLE_WORD, value: sb.toString()]
			command = commands.pop() as CommandParam
			sb = new StringBuilder()

			return true
		}
		else if (command.type == CommandType.OBJECT_NAME) {
			if (sb.length() > 0) tokens << [type: TokenType.OBJECT_NAME, value: sb.toString()]
			command = commands.pop() as CommandParam
			sb = new StringBuilder()

			return true
		}

		return false
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
            command.value = c
            addChar(c)
        }

        input.mark(2)
        def n = input.read()
        if (Character.getType(n) == Character.MATH_SYMBOL || n in [37, 38, 42]) {
            curNum++
            addChar(n)
        } else input.reset()

        if (n == 42) {
            n = input.read()
            if (Character.getType(n) == Character.MATH_SYMBOL || n in [37, 38, 42]) {
                curNum++
                addChar(n)
            } else input.reset()
        }

        if (sb.length() > 0) tokens << [type: TokenType.OPERATOR, value: sb.toString()]
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
			command.value = c
			return
		}

		if (c != command.value) {
			addChar(c)
			return
		}

		input.mark(1)
		def n = input.read()
		if (c == n) {
			curNum++
			addChar(n)
			return
		}
		// If point, change to object name
		else if (n == 46) {
			curNum++
			addChar(n)
			command.type = CommandType.OBJECT_NAME
			return
		}
		input.reset()

		if (sb.length() >= 0) tokens << [type: TokenType.QUOTED_TEXT, quote: ((char)(c as int)).toString(), value: sb.toString()]
		command = commands.pop() as CommandParam
		sb = new StringBuilder()
	}

	/**
	 * Start comments block
	 */
	private void comment_start(Integer c) {
		input.mark(1)
		def n1 = input.read()

		if (command.type == CommandType.QUOTE || n1 != 42) {
			input.reset()
			addChar(c)
			return
		}

		commands.push(command)
		command = new CommandParam()
		command.type = CommandType.COMMENT
		command.start = c
		command.finish = c
	}

	/**
	 * Finish comments block
	 */
	private void comment_finish(Integer c) {
		if (sb.length() >= 0) tokens << [type: TokenType.COMMENT, comment_start: "/*", comment_finish: "*/", value: sb.toString()]
		command = commands.pop() as CommandParam
		sb = new StringBuilder()
	}

	/**
	 * Start bracket level
	 * @param c
	 * @param type
	 */
	private void level_start (Integer c1, Integer c2) {
		if (command.type in [CommandType.QUOTE, CommandType.COMMENT]) {
			addChar(c1)
			return
		}

		gap(c1)

		if (tokens.size() > 0) {
			Map token = (Map)tokens[tokens.size() - 1]
			Map tokenDelimiterType = (Map) token."delimiter"
			if (token."type" == TokenType.SINGLE_WORD && tokenDelimiterType?."type" != TokenType.COMMA) {
				token."type" = TokenType.FUNCTION
			}
		}

		commands.push(command)
		command = new CommandParam()
		command.type = CommandType.BRACKET
		command.start = c1
		command.finish = c2

		stackTokens.push(tokens)
		tokens = []
	}

	private void level_finish (Integer c1, Integer c2) {
		if (command.type in [CommandType.QUOTE, CommandType.COMMENT]) {
			addChar(c2)
			return
		}

		gap(c2)

		if (command.type != CommandType.BRACKET || command.start != c1 || command.finish != c2) error("opening bracket not found")

		List curTokens = tokens
		tokens = stackTokens.pop() as List

		Map prevToken = [:]
		if (tokens.size() != 0) prevToken = (Map) tokens.get(tokens.size() - 1)

		if (tokens.size() > 0 && prevToken."type" == TokenType.FUNCTION && (prevToken?."delimiter" as Map)?."type" != TokenType.COMMA) {
			prevToken."list" = curTokens
			prevToken."start" = ((char)(c1 as int)).toString()
			prevToken."finish" = ((char)(c2 as int)).toString()
		}
		else {
			tokens << [type: TokenType.LIST, start: ((char)(c1 as int)).toString(), finish: ((char)(c2 as int)).toString(), list: curTokens]
		}

		command = commands.pop() as CommandParam
	}

	private void comma(Integer c) {
		if (command.type in [CommandType.QUOTE, CommandType.COMMENT]) {
			addChar(c)
			return
		}

		if (!gap(c) && !(command.type in [CommandType.NONE, CommandType.BRACKET])) error("unexpected comma")
		Map m = [type: TokenType.COMMA, value: ((char)(c as int)).toString()]
		tokens[tokens.size() - 1]."delimiter" = m
	}

	private void semicolon(Integer c) {
		if (command.type in [CommandType.QUOTE, CommandType.COMMENT]) {
			addChar(c)
			return
		}

		gap(c)

		tokens << [type: TokenType.SEMICOLON, value: ((char)(c as int)).toString()]
	}

	protected error(String message) {
		throw new ExceptionGETL("Syntax error line $curLine, col $curNum: $message, current command: type=${command.type};value=${command.value};start=${command.start};finish=${command.finish};buffer=${sb.toString()}")
	}

	@Override
	String toString() {
		MapUtils.ToJson([tokens: tokens])
	}

	/**
	 * Return list of statements separated by a semicolon
	 * @return
	 */
	List<List<Map>> statements () {
		List<List<Map>> res = []
		def cur = 0
		def pos = findByType(tokens, TokenType.SEMICOLON, 0)
		while (pos != -1) {
			if (pos >= cur) res << tokens.subList(cur, pos)
			cur = pos + 1
			pos = findByType(tokens, TokenType.SEMICOLON, cur)
		}

		res
	}

	/**
	 * Return key words for start token while words is single word
	 * @param start
	 * @return
	 */
	static String keyWords(List<Map> tokens, Integer start, Integer max) {
		StringBuilder sb = new StringBuilder()
		def i = 0
		while (start < tokens.size() && tokens[start]."type" == TokenType.SINGLE_WORD && (max == null || i < max)) {
			i++
			sb << tokens[start]."value"
			sb << " "
			start++
		}

		(sb.length() > 0)?sb.substring(0, sb.length() - 1):""
	}

	/**
	 * Return list for position token
	 * @param position
	 * @return
	 */
	static List<Map> list (List<Map> tokens, Integer position) {
		(List<Map>)((tokens[position]."type" == TokenType.LIST)?tokens[position]."list":null)
	}

	/**
	 * Return function name and parameters for position token
	 * @param tokens
	 * @param position
	 * @return
	 */
	static Map function (List<Map> tokens, Integer position) {
		Map token = tokens[position]
		if (token."type" != TokenType.FUNCTION) return null

		token
	}

	/**
	 * Return object name for start token while has single or double quoted word delimiters by point
	 * @param start
	 * @param res
	 * @return - next token after object name
	 */
	static List object (List<Map> tokens, Integer start) {
		if (!((tokens[start]."type" as TokenType) in [TokenType.SINGLE_WORD, TokenType.OBJECT_NAME])) {
			return null
		}

		((String)tokens[start]."value").split("[.]").toList()
	}

	/**
	 * Return token type
	 * @param position
	 * @return
	 */
	static TokenType type (List<Map> tokens, Integer position) {
		(TokenType)((position < tokens.size())?tokens[position]."type":null)
	}

	/**
	 * Find token by type
	 * @param type
	 * @param start
	 * @return
	 */
	static Integer findByType (List<Map> tokens, TokenType type, Integer start) {
		for (Integer i = start; i < tokens.size(); i++) {
			if (tokens[i]."type" == type) return i
		}
		return -1
	}


	/**
	 * Convert tokens with comma separate to list
	 * @param tokens
	 * @param start
	 * @param finish
	 * @return
	 */
	static List<List<Map>> toList (List<Map> tokens, Integer start, Integer finish) {
		List<List<Map>> res = new ArrayList<List<Map>>()
		List<Map> cur = new ArrayList<Map>()
		while (start <= finish && tokens[start]."type"  != TokenType.SEMICOLON) {
			def token = tokens[start]
			cur << token
			Map m = (Map)token."delimiter"
			if (m?."type" == TokenType.COMMA || start == finish) {
				res << cur
				cur = new ArrayList<Map>()
			}

			start++
		}
		if (!cur.isEmpty()) res << cur

		res
	}

	/**
	 * Convert tokens with key word delimiter to list
	 * @param tokens
	 * @param start
	 * @param finish
	 * @param delimiter
	 * @return
	 */
	static List<List<Map>> toList (List<Map> tokens, Integer start, Integer finish, String delimiter) {
		delimiter = delimiter.toUpperCase()
		List<List<Map>> res = new ArrayList<List<Map>>()
		List<Map> cur = new ArrayList<Map>()
		while (start <= finish && tokens[start]."type"  != TokenType.SEMICOLON) {
			def token = tokens[start]
			if ((token."type" == TokenType.SINGLE_WORD && ((String)token."value").toUpperCase() == delimiter)) {
				res << cur
				cur = new ArrayList<Map>()
			}
			else {
				cur << token
			}

			start++
		}
		if (!cur.isEmpty()) res << cur

		res
	}

	/**
	 * Return position key word by start position
	 * @param tokens
	 * @param start
	 * @param keyWord
	 * @return
	 */
	static Integer findKeyWord(List<Map> tokens, Integer start, String keyWord) {
		keyWord = keyWord.toUpperCase()
		for (Integer i = start; i < tokens.size(); i++) {
			def token = tokens[i]
			if (token."type" == TokenType.SINGLE_WORD && ((String)token."value").toUpperCase() == keyWord) return i
		}

		return -1
	}
}