/*
 GETL - based package in Groovy, which automates the work of loading and transforming data. His name is an acronym for "Groovy ETL".

 GETL is a set of libraries of pre-built classes and objects that can be used to solve problems unpacking,
 transform and load data into programs written in Groovy, or Java, as well as from any software that supports
 the work with Java classes.
 
 Copyright (C) 2013-2015  Alexsey Konstantonov (ASCRUS)

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

package getl.utils

import getl.exception.ExceptionGETL
import java.util.logging.*

/**
 * Log manager class
 * @author Alexsey Konstantinov
 *
 */
class Logs {
	Logs () {
		throw new ExceptionGETL("Deny create instance Logs class")
	}
	
	/**
	 * Logger
	 */
	public static final Logger logger = Logger.getLogger("global")
	
	/**
	 * Formater log
	 */
	public static final LogFormatter formatter = new LogFormatter()

	/**
	 * Print config message	
	 */
	public static Boolean printConfigMessage
	
	/**
	 * File handler
	 */
	public static FileHandler file
	
	/**
	 * Log file name
	 */
	public static String logFileName
	
	/**
	 * Log file handler
	 * @return
	 */
	private static String fileNameHandler
	public static String getFileNameHandler () { fileNameHandler }
	
	/**
	 * Config messages to be written after initialization log
	 */
	protected static List<String> InitMessages = []
	
	/**
	 * Eventer on call write to log, has parameters String level, Date time, String message
	 */
	private static final List<Closure> eventers = []

	/**
	 * Print stack trace for error
	 */
	public static boolean printStackTraceError = true
	
	/**
	 * Register eventer, closure has parameters String level, Date time, String message
	 * @param eventer
	 * @return
	 */
	public static void registerEventer (Closure eventer) {
		if (eventer == null) throw new ExceptionGETL("Eventer must be not null")
		eventers << eventer
	}
	
	/**
	 * Unregister eventer
	 * @param eventer
	 * @return
	 */
	public static boolean unregisterEventer (Closure eventer) {
		if (eventer == null) throw new ExceptionGETL("Eventer must be not null")
		eventers.remove(eventer)
	}
	
	/**
	 * Call eventers
	 * @param level
	 * @param time
	 * @param message
	 */
	@groovy.transform.Synchronized
	protected static void event (java.util.logging.Level level, String message) {
		eventers.each { Closure eventer -> eventer(level.toString(), DateUtils.Now(), message) }
	}
	
	/**
	 * Log formatter 
	 * @author Alexsey Konstantinov
	 *
	 */
	static class LogFormatter extends Formatter {
		public String format(LogRecord record)  {
			StringBuilder sb = new StringBuilder()
			Date d = DateUtils.ToOrigTimeZoneDate(new Date(record.millis))
			sb << DateUtils.FormatDate('yyyy-MM-dd HH:mm:ss', d)
			sb << ' ['
			sb << record.level
			sb << ']:	'
			sb << record.message
			sb << '\n'
			
			sb.toString()
		}
	}
	
	/**
	 * Error console handler
	 */
	protected static ConsoleHandler consoleHandler
	
	/**
	 * Init log on create
	 */
	protected static void DoInit() {
		if (consoleHandler != null) return
		
		logger.setUseParentHandlers(false)
		logger.setLevel(Level.FINE)
		
		ConsoleHandler ch = new ConsoleHandler()
		ch.level = Level.SEVERE
		ch.setFormatter(formatter)
		logger.addHandler(ch)
		
		consoleHandler = ch
	} 
	
	/**
	 * Init log on load config
	 */
	public static void Init () {
		if (printConfigMessage == null) printConfigMessage = ((Config.content.log as Map)?.printConfig != null)?(Config.content.log as Map).printConfig:false
		InitFile(logFileName?:(Config.content.log as Map)?.file)
		InitMessages.each { Config(it) }
		InitMessages = []
	}
	
	/**
	 * Init log file
	 * @param name
	 */
	public static void InitFile (String name) {
		if (name != null) {
			if (file != null) logger.removeHandler(file)

			name = name.replace("\\", "\\\\")
			def f = StringUtils.EvalMacroString(name, StringUtils.MACROS_FILE)
			FileUtils.ValidFilePath(f)

			file = new FileHandler(f, true)
			file.level = Level.INFO
			file.setFormatter(formatter)
			logger.addHandler(file)
			fileNameHandler = f
		}
		
		DoInit()
		
		if (name != null) Config("Log file opened")
	}
	
	/**
	 * Done on close log
	 */
	public static void Done () {
		if (file == null) return
		Config("Log file closed")
		logger.removeHandler(file)
		file.close()
		file = null
	}
	
	/**
	 * Convert level name to level value
	 * @param level
	 * @return
	 */
	public static Level StrToLevel(String level) {
		Level result
		switch (level.trim().toUpperCase()) {
			case "ALL":
				result = Level.ALL
				break
			
			case "CONFIG":
				result = Level.CONFIG 
				break
			
			case "FINE":
				result = Level.FINE
				break
			
			case "FINER":
				result = Level.FINER
				break
			
			case "FINEST":
				result = Level.FINEST
				break
			
			case "INFO":
				result = Level.INFO
				break
			
			case "OFF":
				result = Level.OFF
				break
			
			case "SEVERE":
				result = Level.SEVERE
				break
			
			case "WARNING":
				result = Level.WARNING
				break
			
			default:
				throw new ExceptionGETL("Can not convert string \"${level}\" to log level")
		}
		result
	}
	
	/**
	 * Format log message
	 * @param message
	 * @return
	 */
	public static String FormatMessage(String message) {
		message?.replace("\n", " ")
	}
	
	/**
	 * Println log message to console
	 * @param level
	 * @param message
	 */
	@groovy.transform.Synchronized
	public static void ToOut(Level level, String message) {
//		if (errStream != null && level == Level.SEVERE) return
		if (level == Level.OFF) return
		def lr = new LogRecord(level, FormatMessage(message))
		print formatter.format(lr)
	}
	
	@groovy.transform.Synchronized
	public static void Fine (String message) {
		ToOut(Level.FINE, message)
		def msg = FormatMessage(message)
		logger.fine(msg)
		event(Level.FINE, message)
	}
	
	@groovy.transform.Synchronized
	public static void Finer (String message) {
		ToOut(Level.FINER, message)
		def msg = FormatMessage(message)
		logger.finer(msg)
		event(Level.FINER, message)
	}
	
	@groovy.transform.Synchronized
	public static void Finest (String message) {
		ToOut(Level.FINEST, message)
		def msg = FormatMessage(message)
		logger.finest(msg)
		event(Level.FINEST, message)
	}
	
	@groovy.transform.Synchronized
	public static void Info (String message) {
		ToOut(Level.INFO, message)
		def msg = FormatMessage(message)
		logger.info(msg)
		event(Level.INFO, message)
	}
	
	@groovy.transform.Synchronized
	public static void Warning (String message) {
		ToOut(Level.WARNING, message)
		def msg = FormatMessage(message)
		logger.warning(msg)
		event(Level.WARNING, message)
	}
	
	@groovy.transform.Synchronized
	public static void Warning (Throwable e) {
		ToOut(Level.WARNING, e.message)
		org.codehaus.groovy.runtime.StackTraceUtils.sanitize(e)
		def t = (e.stackTrace.length > 0)?" => " + e.stackTrace[0]:""
		def msg = e.getClass().name + ": " + FormatMessage(e.message) + t
		logger.warning(msg)
		event(Level.WARNING, e.message)
	}
	
	@groovy.transform.Synchronized
	public static void Severe (String message) {
		ToOut(Level.SEVERE, message)
		def msg = FormatMessage(message)
		logger.severe(msg)
		event(Level.SEVERE, message)
	}
	
	@groovy.transform.Synchronized
	public static void Exception (Throwable e) {
		ToOut(Level.SEVERE, e.message)
		org.codehaus.groovy.runtime.StackTraceUtils.sanitize(e)
		def t = (e.stackTrace.length > 0)?" => " + e.stackTrace[0]:""
		def message = FormatMessage(e.message + t)
		logger.severe(message)
		event(Level.SEVERE, e.message)
	}
	
	@groovy.transform.Synchronized
	public static void Exception (Throwable e, String typeObject, String nameObject) {
		ToOut(Level.SEVERE, e.message)
		def t = (e.stackTrace.length > 0)?" => " + e.stackTrace[0]:""
		def message = "<${typeObject} ${nameObject}> ${e.getClass().name}: ${FormatMessage(e.message)}${t}"
		logger.severe(message)
		event(Level.SEVERE, e.message)
		org.codehaus.groovy.runtime.StackTraceUtils.sanitize(e)
		if (printStackTraceError) e.printStackTrace()
	}
	
	@groovy.transform.Synchronized
	public static void Entering (String sourceClass, String sourceMethod, Object[] params) {
		logger.entering(sourceClass, sourceMethod, params)
	}
	
	@groovy.transform.Synchronized
	public static void Exiting (String sourceClass, String sourceMethod, Object result) {
		logger.exiting(sourceClass, sourceMethod, result)
	}
	
	@groovy.transform.Synchronized
	public static void Write(Level level, String message) {
		if (level == Level.OFF) return
		ToOut(level, message)
		def msg = FormatMessage(message)
		logger.log(level, msg)
		event(level, message)
	}
	
	@groovy.transform.Synchronized
	public static void Write(String level, String message) {
		def l = StrToLevel(level)
		if (l == Level.OFF) return
		ToOut(l, message)
		def msg = FormatMessage(message)
		logger.log(l, msg)
		event(l, message)
	}
	
	@groovy.transform.Synchronized
	public static String DumpFolder() {
		FileUtils.ConvertToUnixPath("${FileUtils.PathFromFile(fileNameHandler)}/dump/${FileUtils.FileName(fileNameHandler)}")
	}
	
	@groovy.transform.Synchronized
	public static void Dump (Throwable e, String typeObject, String nameObject, def data) {
		if (fileNameHandler == null) {
			Severe("Can not save dump, required logFileName")
			println data
			return
		}
		
		def fn = "${DumpFolder()}/dump.txt"
		
		Fine("Save dump information to ${fn} with error ${e.message}")
		FileUtils.ValidFilePath(fn)
		if (e != null) org.codehaus.groovy.runtime.StackTraceUtils.sanitize(e)
		File df = new File(fn)
		def w
		try {
			w = new PrintWriter(df.newWriter("UTF-8", true))
			w.println "******************** Time: ${DateUtils.FormatDateTime(DateUtils.Now())} ********************"
			w.println "Type: ${typeObject}"
			w.println "Name: ${nameObject}"
			if (e != null) {
				w.println "Error: $e"
				w.println "Stack trace:"
				org.codehaus.groovy.runtime.StackTraceUtils.sanitize(e)
				if (printStackTraceError) e.printStackTrace(w)
			}
			if (data != null) {
				w.println "Generated script:"
				w.println data
			}
			w.println "\n\n\n"
		}
		catch (Throwable error) {
			println "Can not write error to dump file \"${fn}\", error: ${error.message}"
			Severe("Can not write error to dump file \"${fn}\", error: ${error.message}")
		}
		finally {
			if (w != null) w.close()
		}
	}
	
	public static void Config(String message) {
		if (printConfigMessage)
			if (logger.handlers.size() == 0) InitMessages << message else Write(Level.CONFIG, message)
	}
	
	/**
	 * Standart console output
	 */
	private static PrintStream standartConsole = System.out
	
	/**
	 * Standart console error
	 */
	private static PrintStream errConsole = System.err 
	
	/**
	 * Output console file name
	 */
	private static String outFileName
	
	/**
	 * Error console file name
	 */
	private static String errFileName
	
	/**
	 * Output console stream
	 */
	private static PrintStream outStream
	
	/**
	 * Error console stream
	 */
	private static PrintStream errStream
	
	/**
	 * Redirect output console to file or standart
	 * @param fileName
	 */
	public static void RedirectStdOut(String fileName) {
		if (fileName != null) {
			println "Redirect console out to file \"$fileName\""
			FileUtils.ValidFilePath(fileName)
			PrintStream ps = new PrintStream(new BufferedOutputStream(new FileOutputStream(fileName)), true) 
			System.setOut(ps)
			this.outFileName = fileName
			this.outStream = ps
		}
		else if (outFileName != null) {
			println "Redirect console out to standart"
			System.setOut(standartConsole)
			this.outFileName = null
			this.outStream = null
		}
	}
	
	public static void RedirectErrOut(String fileName) {
		if (fileName != null) {
			println "Redirect error out to file \"$fileName\""
			FileUtils.ValidFilePath(fileName)
			PrintStream ps = new PrintStream(new BufferedOutputStream(new FileOutputStream(fileName)), true)
			System.setErr(ps)
			this.errFileName = fileName
			this.errStream = ps
		}
		else if (errFileName != null) {
			println "Redirect error out to standart"
			System.setErr(errConsole)
			this.errFileName = null
			this.errStream = null
		}
	}
}
