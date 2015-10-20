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

import getl.exception.ExceptionGETL

/**
 * Analize text as lexer
 * @author Alexsey Konstantinov
 *
 */
@groovy.transform.CompileStatic
class Lexer {
	public static enum TokenType {SINGLE_WORD, QUOTED_TEXT, LIST, COMMA, SEMICOLON, FUNCTION, OBJECT_NAME}
	
	/**
	 * Type of use parse command
	 */
	private static enum CommandType {NONE, WORD, QUOTE, BRACKET, OBJECT_NAME}

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
	 * @author owner
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
	private Stack<CommandParam> commands
	
	/**
	 * Current text 
	 */
	private StringBuilder sb
	
	/**
	 * Stack tokens by lists
	 */
	private Stack<List> stackTokens
	
	/**
	 * Current column number in line 
	 */
	private int curNum
	
	/**
	 * Current line
	 */
	private int curLine
	
	/**
	 * Parse input stream
	 */
	public void parse () {
		tokens = []
		command = new CommandParam()
		command.type = CommandType.NONE
		commands = new Stack<CommandParam>()
		stackTokens = new Stack<List>()
		sb = new StringBuilder()
		
		curNum = 0
		curLine = 1
		
		int c = input.read()
		while (c != -1) {
			curNum++
			switch (c) {
				// Tab
				case 9:
					curNum += 3 
					if (command.type == CommandType.QUOTE) addChar(c) else gap(c)
					break
				// Space
				case 32:  
					if (command.type == CommandType.QUOTE) addChar(c) else gap(c)
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
					if (command.type == CommandType.QUOTE) addChar(c) else gap(c)
					break
				default:
					if (!(command.type in [CommandType.QUOTE, CommandType.WORD, CommandType.OBJECT_NAME])) {
						commands.push(command)
						command = new CommandParam()
						command.type = CommandType.WORD
					}
					addChar(c)
			}
			
			c = input.read()
		}
	}
	
	private void addChar(int c) {
		sb << (char)c
	}
	
	/**
	 * Gap current word
	 * @param c
	 * @return
	 */
	private boolean gap (int c) {
		if (command.type == CommandType.WORD) {
			if (sb.length() > 0) tokens << [type: TokenType.SINGLE_WORD, value: sb.toString()]
			command = commands.pop()
			sb = new StringBuilder()
		
			return true
		}
		else if (command.type == CommandType.OBJECT_NAME) {
			if (sb.length() > 0) tokens << [type: TokenType.OBJECT_NAME, value: sb.toString()]
			command = commands.pop()
			sb = new StringBuilder()
		
			return true
		}
		
		return false
	}
	
	/**
	 * Valid quoted text
	 * @param c
	 * @param type
	 */
	private void quote(int c) {
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
		int n = input.read()
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
		
		if (sb.length() > 0) tokens << [type: TokenType.QUOTED_TEXT, quote: ((char)c).toString(), value: sb.toString()]
		command = commands.pop()
		sb = new StringBuilder()
	}
	
	/**
	 * Start bracket level
	 * @param c
	 * @param type
	 */
	private void level_start (int c1, int c2) {
		if (command.type == CommandType.QUOTE) {
			addChar(c1)
			return
		}
		
		gap(c1)
		
		if (tokens.size() > 0) {
			Map token = (Map)tokens[tokens.size() - 1]
			if (token."type" == TokenType.SINGLE_WORD) {
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
	
	private void level_finish (int c1, int c2) {
		if (command.type == CommandType.QUOTE) {
			addChar(c2)
			return
		}
		
		gap(c2)

		if (command.type != CommandType.BRACKET || command.start != c1 || command.finish != c2) error("opening bracket not found")
		
		List curTokens = tokens
		tokens = stackTokens.pop()
		if (tokens.size() > 0 && ((Map)(tokens[tokens.size() - 1]))."type" == TokenType.FUNCTION) {
			Map token = (Map)tokens[tokens.size() - 1]
			token."list" = curTokens
			token."start" = ((char)c1).toString()
			token."finish" = ((char)c2).toString()
		}
		else {
			tokens << [type: TokenType.LIST, start: ((char)c1).toString(), finish: ((char)c2).toString(), list: curTokens]
		}
		
		command = commands.pop()
	}
	
	private void comma(int c) {
		if (command.type == CommandType.QUOTE) {
			addChar(c)
			return
		}

		if (!gap(c) && !(command.type in [CommandType.NONE, CommandType.BRACKET])) error("unexpected comma")
		Map m = [type: TokenType.COMMA, value: ((char)c).toString()]
		tokens[tokens.size() - 1]."delimiter" = m
	}
	
	private void semicolon(int c) {
		if (command.type == CommandType.QUOTE) {
			addChar(c)
			return
		}

		gap(c)
		
		tokens << [type: TokenType.SEMICOLON, value: ((char)c).toString()]
	}
	
	protected error(String message) {
		throw new ExceptionGETL("Syntax error line $curLine, col $curNum: $message, current command: type=${command.type};value=${command.value};start=${command.start};finish=${command.finish};buffer=${sb.toString()}")
	}
	
	@Override
	public String toString() {
		MapUtils.ToJson([tokens: tokens])
	}
	
	/**
	 * Return list of statements separated by a semicolon
	 * @return
	 */
	public List<List<Map>> statements () {
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
	public String keyWords(List<Map> tokens, int start, Integer max) {
		StringBuilder sb = new StringBuilder()
		int i = 0
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
	public List<Map> list (List<Map> tokens, int position) {
		(List<Map>)((tokens[position]."type" == TokenType.LIST)?tokens[position]."list":null)
	}
	
	/**
	 * Return function name and parameters for position token 
	 * @param tokens
	 * @param position
	 * @return
	 */
	public Map function (List<Map> tokens, int position) {
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
	public List object (List<Map> tokens, int start) {
		if (!(tokens[start]."type" in [TokenType.SINGLE_WORD, TokenType.OBJECT_NAME])) {
			return null
		}

		((String)tokens[start]."value").split("[.]").toList()
	}
	
	/**
	 * Return token type 
	 * @param position
	 * @return
	 */
	public TokenType type (List<Map> tokens, int position) {
		(TokenType)((position < tokens.size())?tokens[position]."type":null)
	}
	
	/**
	 * Find token by type
	 * @param type
	 * @param start
	 * @return
	 */
	public int findByType (List<Map> tokens, TokenType type, int start) {
		for (int i = start; i < tokens.size(); i++) {
			if (tokens[i]."type" == type) return i
		}
		return -1
	}
	
	
	/**
	 * Convert tokens with comma sepatate to list
	 * @param tokens
	 * @param start
	 * @param finish
	 * @return
	 */
	public List<List<Map>> toList (List<Map> tokens, int start, int finish) {
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
	public List<List<Map>> toList (List<Map> tokens, int start, int finish, String delimiter) {
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
	public int findKeyWord(List<Map> tokens, int start, String keyWord) {
		keyWord = keyWord.toUpperCase()
		for (int i = start; i < tokens.size(); i++) {
			def token = tokens[i]
			if (token."type" == TokenType.SINGLE_WORD && ((String)token."value").toUpperCase() == keyWord) return i 
		} 
		
		return -1
	} 
}
