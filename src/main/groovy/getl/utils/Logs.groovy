/*
 GETL - based package in Groovy, which automates the work of loading and transforming data. His name is an acronym for "Groovy ETL".

 GETL is a set of libraries of pre-built classes and objects that can be used to solve problems unpacking,
 transform and load data into programs written in Groovy, or Java, as well as from any software that supports
 the work with Java classes.
 
 Copyright (C) EasyData Company LTD

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
import groovy.transform.Synchronized
import org.codehaus.groovy.runtime.StackTraceUtils

import java.util.logging.*

/**
 * Java logger manager class
 * @author Alexsey Konstantinov
 *
 */
class Logs {
	Logs () {
		throw new ExceptionGETL("Deny create instance Logs class")
	}
	
	/**
	 * Logger object
	 */
	public static final Logger logger = Logger.getLogger("global")
	
	/**
	 * Formatter object
	 */
	public static final LogFormatter formatter = new LogFormatter()

	/**
	 * Display configuration messages
	 */
	public static Boolean printConfigMessage
	
	/**
	 * File handler object
	 */
	public static FileHandler file
	
	/**
	 * Log file name
	 * <br>The following macro variables are allowed (values are calculated at the start of the program):
	 * <ul>
	 *     <li>{date} - the current date in the format "yyyy-MM-dd"</li>
	 *     <li>{monthdate} - the current month in the format "yyyy-MM"</li>
	 *     <li>{yeardate} - the current year in the format "yyyy"</li>
	 *     <li>{time} - the current time in the format "HH-mm-ss"</li>
	 *     <li>{shorttime} - the current time without seconds in the format "HH-mm"</li>
	 *     <li>{hour} - the current hour in the format "HH"</li>
	 *     <li>{datetime} - the current date and time in the format "yyyy-MM-dd_HH-mm-ss"</li>
	 *     <li>{shortdatetime} - the current date and time in the format "yyyy-MM-dd_HH"</li>
	 * </ul>
	 */
	public static String logFileName
	
	/** Log file handler */
	private static String fileNameHandler

	/** Log file handler */
	static String getFileNameHandler () { fileNameHandler }
	
	/** Config messages to be written after initialization log */
	protected static List<String> InitMessages = []
	
	/** Eventer on call write to log, has parameters String level, Date time, String message */
	private static final List<Closure> eventers = []

	/** Print stack trace for error */
	public static boolean printStackTraceError = true
	
	/**
	 * Register eventer, closure has parameters String level, Date time, String message
	 * @param eventer event code
	 */
	static void registerEventer (Closure eventer) {
		if (eventer == null) throw new ExceptionGETL("Eventer must be not null")
		eventers << eventer
	}
	
	/**
	 * Unregister eventer
	 * @param eventer event code
	 * @return sucess unregistered
	 */
	static boolean unregisterEventer (Closure eventer) {
		if (eventer == null) throw new ExceptionGETL("Eventer must be not null")
		eventers.remove(eventer)
	}
	
	/**
	 * Call eventers
	 * @param level
	 * @param time
	 * @param message
	 */
	@Synchronized
	protected static void event (Level level, String message) {
		eventers.each { Closure eventer -> eventer(level.toString(), DateUtils.Now(), message) }
	}
	
	/**
	 * Log formatter extend class
	 * @author Alexsey Konstantinov
	 *
	 */
	static class LogFormatter extends Formatter {
		String format(LogRecord record)  {
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
	
	/** Error console handler */
	protected static ConsoleHandler consoleHandler
	
	/** Initialize on create log object */
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
	
	/** Initialize log after load config */
	static void Init () {
		if (printConfigMessage == null) printConfigMessage = ((Config.content.log as Map)?.printConfig != null)?(Config.content.log as Map).printConfig:false
		InitFile(logFileName?:((Config.content.log as Map)?.file as String))
		InitMessages.each { Config(it) }
		InitMessages = []
	}
	
	/**
	 * Initialize log file
	 * @param name log file name
	 */
	static void InitFile (String name) {
		if (file != null) {
			file.close()
			logger.removeHandler(file)
		}
		if (name != null) {
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
	 * Finalization when closing the log file
	 */
	static void Done () {
		if (file == null) return
		Config("Log file closed")
		logger.removeHandler(file)
		file.close()
		file = null
	}
	
	/**
	 * Convert level name to level value
	 * @param level log level text value
	 * @return log level value
	 */
	static Level StrToLevel(String level) {
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
	 * @param message text message
	 * @return formatted text message
	 */
	static String FormatMessage(String message) {
		message?.replace("\n", " ")
	}
	
	/**
	 * Println log message to console
	 * @param level log level value
	 * @param message text message
	 */
	@Synchronized
	static void ToOut(Level level, String message) {
		if (level == Level.OFF) return
		def lr = new LogRecord(level, FormatMessage(message))
		print formatter.format(lr)
	}

	/**
	 * Log message with level FINE
	 * @param message text message
	 */
	@Synchronized
	static void Fine (String message) {
		ToOut(Level.FINE, message)
		def msg = FormatMessage(message)
		logger.fine(msg)
		event(Level.FINE, message)
	}

	/**
	 * Log message with level FINER
	 * @param message text message
	 */
	@Synchronized
	static void Finer (String message) {
		ToOut(Level.FINER, message)
		def msg = FormatMessage(message)
		logger.finer(msg)
		event(Level.FINER, message)
	}

	/**
	 * Log message with level FINEST
	 * @param message text message
	 */
	@Synchronized
	static void Finest (String message) {
		ToOut(Level.FINEST, message)
		def msg = FormatMessage(message)
		logger.finest(msg)
		event(Level.FINEST, message)
	}

	/**
	 * Log message with level INFO
	 * @param message text message
	 */
	@Synchronized
	static void Info (String message) {
		ToOut(Level.INFO, message)
		def msg = FormatMessage(message)
		logger.info(msg)
		event(Level.INFO, message)
	}

	/**
	 * Log message with level WARNING
	 * @param message text message
	 */
	@Synchronized
	static void Warning (String message) {
		ToOut(Level.WARNING, message)
		def msg = FormatMessage(message)
		logger.warning(msg)
		event(Level.WARNING, message)
	}

	/**
	 * Log message with level WARNING
	 * @param error error exception
	 */
	@Synchronized
	static void Warning (Throwable error) {
		ToOut(Level.WARNING, error.message)
		StackTraceUtils.sanitize(error)
		def t = (error.stackTrace.length > 0)?" => " + error.stackTrace[0]:""
		def msg = error.getClass().name + ": " + FormatMessage(error.message) + t
		logger.warning(msg)
		event(Level.WARNING, error.message)
	}

	/**
	 * Log message with level SEVERE
	 * @param message text message
	 */
	@Synchronized
	static void Severe (String message) {
		ToOut(Level.SEVERE, message)
		def msg = FormatMessage(message)
		logger.severe(msg)
		event(Level.SEVERE, message)
	}

	/**
	 * Log message with level SEVERE with clearing error tracing
	 * @param error error exception
	 */
	@Synchronized
	static void Exception (Throwable error) {
		ToOut(Level.SEVERE, error.message)
		StackTraceUtils.sanitize(error)
		def t = (error.stackTrace.length > 0)?" => " + error.stackTrace[0]:""
		def message = FormatMessage(error.message + t)
		logger.severe(message)
		event(Level.SEVERE, error.message)
	}

	/**
	 * Log message with level SEVERE with clearing error tracing
	 * @param error error exception
	 * @param typeObject object type name
	 * @param nameObject object name
	 */
	@Synchronized
	static void Exception (Throwable error, String typeObject, String nameObject) {
		ToOut(Level.SEVERE, error.message)
		def message = "<${typeObject} ${nameObject}> ${error.getClass().name}: ${FormatMessage(error.message)}"
		if (error.stackTrace.length > 0) {
			message += " => " + StringUtils.ToText(error.stackTrace[0].toString())[0]
		}
		logger.severe(message)
		event(Level.SEVERE, error.message)
		StackTraceUtils.sanitize(error)
		if (printStackTraceError) error.printStackTrace()
	}

	/**
	 * Log message with level ENTERING for specified method
	 */
	@Synchronized
	static void Entering (String sourceClass, String sourceMethod, Object[] params) {
		logger.entering(sourceClass, sourceMethod, params)
	}

	/**
	 * Log message with level EXITING for specified method
	 */
	@Synchronized
	static void Exiting (String sourceClass, String sourceMethod, Object result) {
		logger.exiting(sourceClass, sourceMethod, result)
	}

	/**
	 * Log message with specified level
	 * @param level log level value
	 * @param message text message
	 */
	@Synchronized
	static void Write(Level level, String message) {
		if (level == Level.OFF) return
		ToOut(level, message)
		def msg = FormatMessage(message)
		logger.log(level, msg)
		event(level, message)
	}

	/**
	 * Log message with specified level
	 * @param level log level value
	 * @param message text message
	 */
	@Synchronized
	static void Write(String level, String message) {
		def l = StrToLevel(level)
		if (l == Level.OFF) return
		ToOut(l, message)
		def msg = FormatMessage(message)
		logger.log(l, msg)
		event(l, message)
	}

	/**
	 * Return the name of the log file dump
	 * @return path to the log file dump
	 */
	@Synchronized
	static String DumpFolder() {
		FileUtils.ConvertToUnixPath("${FileUtils.PathFromFile(fileNameHandler)}/dump/${FileUtils.FileName(fileNameHandler)}")
	}

	/**
	 * Write error trace to dump log file
	 * @param error error exception
	 * @param typeObject object type name
	 * @param nameObject object name
	 * @param data additional error data
	 */
	@Synchronized
	static void Dump (Throwable error, String typeObject, String nameObject, def data) {
		if (fileNameHandler == null) {
			Severe("Can not save dump, required logFileName")
			println data.toString()
			return
		}
		
		def fn = "${DumpFolder()}/dump.txt"
		
		Fine("Save dump information to ${fn} with error ${error.message}")
		FileUtils.ValidFilePath(fn)
		if (error != null) StackTraceUtils.sanitize(error)
		File df = new File(fn)
		def w
		try {
			w = new PrintWriter(df.newWriter("UTF-8", true))
			w.println "******************** Time: ${DateUtils.FormatDateTime(DateUtils.Now())} ********************"
			w.println "Type: ${typeObject}"
			w.println "Name: ${nameObject}"
			if (error != null) {
				w.println "Error: $error"
				w.println "Stack trace:"
				StackTraceUtils.sanitize(error)
				if (printStackTraceError) error.printStackTrace(w)
			}
			if (data != null) {
				w.println "Generated script:"
				w.println data.toString()
			}
			w.println "\n\n\n"
		}
		catch (Throwable e) {
			println "Can not write error to dump file \"${fn}\", error: ${e.message}"
			Severe("Can not write error to dump file \"${fn}\", error: ${e.message}")
		}
		finally {
			if (w != null) w.close()
		}
	}

	/**
	 * Log message with level CONFIG
	 * @param message text message
	 */
	static void Config(String message) {
		if (printConfigMessage)
			if (logger.handlers.size() == 0) InitMessages << message else Write(Level.CONFIG, message)
	}
	
	/** Standart console output */
	private static PrintStream standartConsole = System.out
	
	/** Standart console error */
	private static PrintStream errConsole = System.err 
	
	/** Output console file name */
	private static String outFileName
	
	/** Error console file name */
	private static String errFileName
	
	/** Output console stream */
	private static PrintStream outStream
	
	/** Error console stream */
	private static PrintStream errStream
	
	/**
	 * Redirect output console to file or standart
	 * @param fileName path to console information output file (if null then the standard console is assigned)
	 */
	static void RedirectStdOut(String fileName) {
		if (fileName != null) {
			println "Redirect console out to file \"$fileName\""
			FileUtils.ValidFilePath(fileName)
			PrintStream ps = new PrintStream(new BufferedOutputStream(new FileOutputStream(fileName)), true) 
			System.setOut(ps)
			outFileName = fileName
			outStream = ps
		}
		else if (outFileName != null) {
			println "Redirect console out to standart"
			System.setOut(standartConsole)
			outFileName = null
			outStream = null
		}
	}

	/**
	 * Redirect errors console to file or standart
	 * @param fileName path to console information output file (if null then the standard console is assigned)
	 */
	static void RedirectErrOut(String fileName) {
		if (fileName != null) {
			println "Redirect error out to file \"$fileName\""
			FileUtils.ValidFilePath(fileName)
			PrintStream ps = new PrintStream(new BufferedOutputStream(new FileOutputStream(fileName)), true)
			System.setErr(ps)
			errFileName = fileName
			errStream = ps
		}
		else if (errFileName != null) {
			println "Redirect error out to standart"
			System.setErr(errConsole)
			errFileName = null
			errStream = null
		}
	}
}
