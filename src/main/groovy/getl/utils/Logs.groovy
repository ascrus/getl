package getl.utils

import com.fasterxml.jackson.annotation.JsonIgnore
import getl.exception.ExceptionGETL
import getl.lang.Getl
import getl.lang.sub.GetlRepository
import groovy.transform.CompileStatic
import groovy.transform.Synchronized
import org.codehaus.groovy.runtime.StackTraceUtils

import java.time.format.DateTimeFormatter
import java.util.logging.*

/**
 * Logger manager class
 * @author Alexsey Konstantinov
 *
 */
@CompileStatic
@SuppressWarnings(['GrMethodMayBeStatic', 'unused'])
class Logs {
	Logs() {
		loggerName = 'global'
		logger = Logger.getLogger(loggerName)
		doInit()
	}

	Logs(String name) {
		loggerName = name
		logger = Logger.getLogger(loggerName)
		doInit()
	}

	/** Messages manager */
	static Messages getMessages() { Messages.manager }

	/** Global instance log */
	static private Logs global
	/** Global instance log */
	@JsonIgnore
	@Synchronized
	static Logs getGlobal() {
		if (global == null)
			Init()

		return global
	}
	/** Global instance log */
	@Synchronized
	static void setGlobal(Logs value) {
		if (global == value)
			return

		if (value == null)
			throw new ExceptionGETL('Global manager can not be null!')

		global = value
	}

	/** Logger name */
	private String loggerName
	/** Logger name */
	@JsonIgnore
	String getLoggerName() { loggerName }

	/** Logger object */
	private Logger logger
	/** Logger object */
	@JsonIgnore
	Logger getLogger() { logger }

	/** Formatter object */
	private LogFormatter formatter = new LogFormatter()
	/** Formatter object */
	@JsonIgnore
	LogFormatter getFormatter() { formatter }

	/** Display configuration messages (default false) */
	private Boolean printConfigMessage = false
	/** Display configuration messages (default false) */
	Boolean getPrintConfigMessage() { printConfigMessage }
	/** Display configuration messages (default false) */
	void setPrintConfigMessage(Boolean value) { printConfigMessage = value }

	/** Display error messages to console (default false) */
	private Boolean printErrorToConsole = false
	/** Display error messages to console (default false) */
	Boolean getPrintErrorToConsole() { printErrorToConsole }
	/** Display error messages to console (default false) */
	void setPrintErrorToConsole(Boolean value) { printErrorToConsole = value }

	/** File handler object */
	private FileHandler file
	/** File handler object */
	@JsonIgnore
	FileHandler getFile() { file }

	/** Log file name */
	private String logFileName
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
	String getLogFileName() { logFileName }
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
	void setLogFileName(String value) {
		logFileName  = value
		initFile(logFileName, logFileLevel)
	}

	/** The level of message logging to console (default FINEST) */
	private Level logConsoleLevel = Level.FINEST
	/** The level of message logging to console (default FINEST) */
	Level getLogConsoleLevel() { logConsoleLevel }
	/** The level of message logging to a file (default FINEST) */
	void setLogConsoleLevel(Level value) {
		if (value == logConsoleLevel)
			return

		logConsoleLevel = value
		if (consoleHandler != null)
			consoleHandler.level = logConsoleLevel?:Level.FINEST
	}

	/** The level of message logging to a file (default INFO) */
	private Level logFileLevel = Level.INFO
	/** The level of message logging to a file (default INFO) */
	Level getLogFileLevel() { logFileLevel }
	/** The level of message logging to a file (default INFO) */
	void setLogFileLevel(Level value) {
		if (value == logFileLevel)
			return

		logFileLevel = value
		if (file != null)
			file.level = logFileLevel?:Level.INFO
	}

	/** Log file handler */
	private String fileNameHandler
	/** Log file handler */
	String getFileNameHandler() { fileNameHandler }
	
	/** Config messages to be written after initialization log */
	protected final List<String> initMessages = [] as List<String>
	
	/** Event on call write to log, has parameters String level, Date time, String message */
	private final List<Closure> events = [] as List<Closure>

	private Boolean printStackTraceError = true
	/** Print stack trace for error (default true) */
	Boolean getPrintStackTraceError() { printStackTraceError }
	/** Print stack trace for error (default true) */
	void setPrintStackTraceError(Boolean value) { printStackTraceError = value }

	private final Object lockLog = new Object()
	private final Object lockOut = new Object()
	
	/**
	 * Register event, closure has parameters String level, Date time, String message
	 * @param event event code
	 */
	void registerEvent(Closure event) {
		if (event == null)
			throw new ExceptionGETL("Event must be not null!")
		events << event
	}
	
	/**
	 * Unregister event
	 * @param event event code
	 * @return success unregistered
	 */
	Boolean unregisterEvent(Closure event) {
		if (event == null) throw new ExceptionGETL("Event must be not null!")
		events.remove(event)
	}

	/**
	 * Call events
	 * @param level
	 * @param time
	 * @param message
	 */
	protected void event(Level level, String message) {
		if (message != null)
			events.each { Closure event -> event.call(level.toString(), DateUtils.Now(), message) }
	}
	
	/**
	 * Log formatter extend class
	 * @author Alexsey Konstantinov
	 *
	 */
	class LogFormatter extends Formatter {
		LogFormatter() {
			dtFormatter = DateUtils.BuildDateTimeFormatter(datetimeFormat)
		}

		private String messageFormat = '{datetime} [{level}]: {message}'
		private String datetimeFormat = 'yyyy-MM-dd HH:mm:ss'
		private DateTimeFormatter dtFormatter

		String getDateTimeFormat() { datetimeFormat }
		@Synchronized
		void setDateTimeFormat(String value) {
			datetimeFormat = value?:'yyyy-MM-dd HH:mm:ss'
			dtFormatter = DateUtils.BuildDateTimeFormatter(datetimeFormat)
		}

		/**
		 * Message format<br>
		 * <b>Supported variables:</b>
		 * <ul>
		 *     <li>datetime</li>
		 *     <li>level</li>
		 *     <li>message</li>
		 *     <li>logger</li>
		 *     <li>parameters</li>
		 *     <li>sequence</li>
		 *     <li>class</li>
		 *     <li>method</li>
		 *     <li>thread</li>
		 *     <li>exception</li>
		 *     <li>error</li>
		 * </ul>
		 */
		String getMessageFormat() { messageFormat }
		@Synchronized
		void setMessageFormat(String value) { messageFormat = value }

		private final Map<String, String> levels = [
		        'OFF':     '    OFF',
				'SEVERE':  ' SEVERE',
				'WARNING': 'WARNING',
				'INFO':    '   INFO',
				'CONFIG':  ' CONFIG',
				'FINE':    '   FINE',
				'FINER':   '  FINER',
				'FINEST':  ' FINEST',
				'ALL':     '    ALL'
		]

		String format(LogRecord record)  {
			def curDateTime = DateUtils.FormatDate(dtFormatter, DateUtils.ToOrigTimeZoneDate(new Date(record.millis)))
			def vars = [
					datetime: curDateTime,
					level: levels.get(record.level.name)?:'UNKNOWN',
					message: record.message,
					logger: record.loggerName,
					parameters: record.parameters,
					sequence: record.sequenceNumber,
					class: record.sourceClassName,
					method: record.sourceMethodName,
					thread: record.threadID
			]
			if (record.thrown != null) {
				vars.exception = record.thrown.getClass().name
				vars.error = record.thrown.message
			}
			else {
				vars.exception = ''
				vars.error = ''
			}
			def res = StringUtils.EvalMacroString(messageFormat, vars) + '\n'
			return res
		}
	}

	class OutConsoleHandler extends StreamHandler {
		OutConsoleHandler(OutputStream out, Formatter f) {
			super(out, f)
		}

		@Override
		synchronized void publish(LogRecord record) {
			if (record.level.intValue() >= Level.SEVERE.intValue())
				return

			super.publish(record)
			flush()
		}

		@Override
		synchronized void close() throws SecurityException {
			flush()
		}
	}
	
	/** Error console handler */
	protected ConsoleHandler errorHandler

	/** Output console handler */
	protected StreamHandler consoleHandler

	/** Initialize on create log object */
	protected void doInit() {
		if (consoleHandler != null)
			return
		
		logger.setUseParentHandlers(false)
		logger.setLevel(Level.ALL)

		consoleHandler = new OutConsoleHandler(System.out, formatter)
		consoleHandler.level = logConsoleLevel?:Level.FINEST
		logger.addHandler(consoleHandler)

		errorHandler = new ConsoleHandler()
		errorHandler.level = Level.SEVERE
		errorHandler.formatter = formatter
		logger.addHandler(errorHandler)
	}

	/** Initialize log after load config */
	static void Init() {
		if (global == null) {
			global = new Logs()
			global.init()
		}
	}
	
	/** Initialize log after load config */
	@Synchronized('lockLog')
	void init() {
		def props = (Config.content.log as Map<String, Object>)?:(new HashMap<String, Object>())

		if (props.printConfig != null)
			printConfigMessage = BoolUtils.IsValue(props.printConfig)

		initFile(logFileName?:props.file as String, logFileLevel?:ObjectToLevel(props.logFileLevel))

		initMessages.each { config(it) }
		initMessages.clear()
	}
	
	/**
	 * Initialize log file
	 * @param name log file name
	 */
	protected void initFile(String name, Level level) {
		String newName = null
		if (name != null) {
			name = name.replace("\\", "\\\\")
			newName = StringUtils.EvalMacroString(name, Config.SystemProps() + StringUtils.MACROS_FILE)
		}
		if (fileNameHandler == newName)
			return

		if (file != null) {
			file.close()
			logger.removeHandler(file)
			fileNameHandler = null
		}
		if (newName == null)
			return

		FileUtils.ValidFilePath(newName)
		file = new FileHandler(newName, true)

		file.level = level?:Level.INFO
		file.setFormatter(formatter)
		logger.addHandler(file)
		fileNameHandler = newName

		if (name != null)
			config("# Log file \"$fileNameHandler\" opened")
	}
	
	/** Finalization when closing the log file */
	static void Done() { global.done() }

	/** Finalization when closing the log file */
	void done() {
		if (file == null)
			return

		logger.removeHandler(file)
		try {
			file.close()
		}
		finally {
			file = null
		}

		config("# Log file \"$fileNameHandler\" closed")

		fileNameHandler = null
	}

	/**
	 * Convert level object to level value
	 * @param level log level object value
	 * @return log level value
	 */
	static Level ObjectToLevel(Object level) {
		return (level instanceof Level)?(level as Level):StrToLevel(level.toString())
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
	 * Println log message to console
	 * @param level log level value
	 * @param message text message
	 */
	static void ToOut(Level level, String message) {
		global.toOut(level, message)
	}

	/**
	 * Println log message to console
	 * @param level log level value
	 * @param message text message
	 */
	void toOut(Level level, String message) {
		if (level == Level.OFF || message == null)
			return

		def lr = new LogRecord(level, message)
		def str = formatter.format(lr)
		synchronized (lockOut) {
			System.out.print(str)
		}
	}

	/**
	 * Log message with level FINE
	 * @param message text message
	 */
	static void Fine(String message) {
		global.fine(message)
	}

	/**
	 * Log message with level FINE
	 * @param message text message
	 */
	void fine(String message) {
		if (message == null)
			return

		synchronized (lockOut) {
			logger.fine(message)
			event(Level.FINE, message)
		}
	}

	/**
	 * Log message with level FINER
	 * @param message text message
	 */
	static void Finer(String message) {
		global.finer(message)
	}

	/**
	 * Log message with level FINER
	 * @param message text message
	 */
	void finer(String message) {
		if (message == null)
			return

		synchronized (lockOut) {
			logger.finer(message)
			event(Level.FINER, message)
		}
	}

	/**
	 * Log message with level FINEST
	 * @param message text message
	 */
	static void Finest(String message) {
		global.finest(message)
	}

	/**
	 * Log message with level FINEST
	 * @param message text message
	 */
	void finest(String message) {
		if (message == null)
			return

		synchronized (lockOut) {
			logger.finest(message)
			event(Level.FINEST, message)
		}
	}

	/**
	 * Log message with level INFO
	 * @param message text message
	 */
	static void Info(String message) {
		global.info(message)
	}

	/**
	 * Log message with level INFO
	 * @param message text message
	 */
	void info(String message) {
		if (message == null)
			return

		synchronized (lockOut) {
			logger.info(message)
			event(Level.INFO, message)
		}
	}

	/**
	 * Log message with level WARNING
	 * @param message text message
	 */
	static void Warning(String message, Throwable e = null) {
		global.warning(message, e)
	}

	/**
	 * Log message with level WARNING
	 * @param message text message
	 */
	void warning(String message, Throwable e = null) {
		if (message == null)
			return

		synchronized (lockOut) {
			logger.warning(StringUtils.FormatException(message, e))
			event(Level.WARNING, message)
		}
	}

	/**
	 * Log message with level WARNING
	 * @param error error exception
	 */
	static void Warning(Throwable error) {
		global.warning(error)
	}

	/**
	 * Log message with level WARNING
	 * @param error error exception
	 */
	void warning(Throwable error) {
		if (error == null)
			return

		StackTraceUtils.sanitize(error)
		def t = (error.stackTrace.length > 0) ? (" => " + error.stackTrace.join('\n')) : ""
		def msg = error.getClass().name + ": " + error.message + t
		synchronized (lockOut) {
			logger.warning(msg)
			event(Level.WARNING, error.message)
		}
	}

	/**
	 * Log message with level SEVERE
	 * @param message text message
	 * @param e exception
	 */
	static void Severe(String message, Throwable e = null) {
		global.severe(message, e)
	}

	/**
	 * Log message with level SEVERE
	 * @param message text message
	 * @param e exception
	 */
	void severe(String message, Throwable e = null) {
		synchronized (lockOut) {
			def txt = StringUtils.FormatException(message, e)
			if (printErrorToConsole)
				toOut(Level.SEVERE, txt)

			logger.severe(txt)
			event(Level.SEVERE, txt)
		}
	}

	/**
	 * Log message with level SEVERE with clearing error tracing
	 * @param error error exception
	 */
	static void Exception(Throwable error) {
		global.exception(error)
	}

	/**
	 * Write error tracing to error output
	 * @param error error exception
	 */
	void exception(Throwable error) {
		if (printStackTraceError) {
			StackTraceUtils.sanitize(error)
			error.printStackTrace()
		}
	}

	/**
	 * Log message with level SEVERE with clearing error tracing
	 * @param error error exception
	 * @param typeObject object type name
	 * @param nameObject object name
	 */
	static void Exception(Throwable error, String typeObject, String nameObject) {
		global.exception(error, typeObject, nameObject)
	}

	/**
	 * Log message with level SEVERE with clearing error tracing
	 * @param error error exception
	 * @param typeObject object type name
	 * @param nameObject object name
	 */
	void exception(Throwable error, String typeObject, String nameObject) {
		severe("<${typeObject} ${nameObject?:'unknown'}> error", error)
		exception(error)
	}

	/**
	 * Log message with level ENTERING for specified method
	 */
	static void Entering(String sourceClass, String sourceMethod, Object[] params) {
		global.entering(sourceClass, sourceMethod, params)
	}

	/**
	 * Log message with level ENTERING for specified method
	 */
	void entering(String sourceClass, String sourceMethod, Object[] params) {
		logger.entering(sourceClass, sourceMethod, params)
	}

	/**
	 * Log message with level EXITING for specified method
	 */
	static void Exiting(String sourceClass, String sourceMethod, Object result) {
		global.exiting(sourceClass, sourceMethod, result)
	}

	/**
	 * Log message with level EXITING for specified method
	 */
	void exiting(String sourceClass, String sourceMethod, Object result) {
		logger.exiting(sourceClass, sourceMethod, result)
	}

	/**
	 * Log message with specified level
	 * @param level log level value
	 * @param message text message
	 */
	static void Write(Level level, String message) {
		global.write(level, message)
	}

	/**
	 * Log message with specified level
	 * @param level log level value
	 * @param message text message
	 */
	void write(Level level, String message) {
		if (level == Level.OFF || message == null)
			return

		synchronized (lockOut) {
			logger.log(level, message)
			event(level, message)
		}
	}

	/**
	 * Log message with specified level
	 * @param level log level value
	 * @param message text message
	 */
	static void Write(String level, String message) {
		global.write(level, message)
	}

	/**
	 * Log message with specified level
	 * @param level log level value
	 * @param message text message
	 */
	void write(String level, String message) {
		def l = StrToLevel(level)
		write(l, message)
	}

	/**
	 * Return the name of the log file dump
	 * @return path to the log file dump
	 */
	static String DumpFolder() { global.dumpFolder() }

	/**
	 * Return the name of the log file dump
	 * @return path to the log file dump
	 */
	@Synchronized('lockLog')
	String dumpFolder() {
		FileUtils.ConvertToUnixPath("${FileUtils.PathFromFile(fileNameHandler)}/dump/${FileUtils.FileName(fileNameHandler)}")
	}

	@Synchronized('lockLog')
	String dumpFile() { "${dumpFolder()}/dump.txt" }

	/**
	 * Write error trace to dump log file
	 * @param error error exception
	 * @param typeObject object type name
	 * @param nameObject object name
	 * @param data additional error data
	 */
	static void Dump(Throwable error, String typeObject, String nameObject, def data) {
		global.dump(error, typeObject, nameObject, data)
	}

	/**
	 * Write error trace to dump log file
	 * @param error error exception
	 * @param typeObject object type name
	 * @param nameObject object name
	 * @param data additional error data
	 */
	@Synchronized('lockLog')
	void dump(Throwable error, String typeObject, String nameObject, Object data) {
		if (fileNameHandler == null) {
			severe("Can not save dump, required logFileName!")
			println data.toString()
			return
		}

		String errorText = 'unknown'
		if (error != null)
			errorText = (error instanceof ExceptionGETL)?error.message:StringUtils.CutStrByLines(error.message, 1, 1024)

		def fn = dumpFile()
		fine("Saving dump information to file $fn, error: $errorText")
		FileUtils.ValidFilePath(fn)

		File df = new File(fn)
		PrintWriter w
		try {
			w = new PrintWriter(df.newWriter("UTF-8", true))
			w.println "******************** Time: ${DateUtils.FormatDateTime(DateUtils.Now())} ********************"
			w.println "Type: ${typeObject}"
			w.println "Name: ${nameObject}"
			if (error != null) {
				w.println "Error: $error"
				w.println "Stack trace:"
				StackTraceUtils.sanitize(error)
				error.printStackTrace(w)
			}
			if (data != null) {
				w.println "Generated script:"
				w.println data.toString()
			}
			w.println "\n\n\n"
		}
		catch (Throwable e) {
			severe("Can not write error to dump file \"${fn}\" with error $errorText", e)
		}
		finally {
			if (w != null)
				w.close()
		}
	}

	/**
	 * Log message with level CONFIG
	 * @param message text message
	 */
	static void Config(String message) {
		global.config(message)
	}

	/**
	 * Log message with level CONFIG
	 * @param message text message
	 */
	void config(String message) {
		if (printConfigMessage)
			if (logger.handlers.size() == 0)
				initMessages << message
			else
				write(Level.CONFIG, message)
	}
	
	/** Standard console output */
	private PrintStream standardConsole = System.out
	
	/** Standard console error */
	private PrintStream errConsole = System.err
	
	/** Output console file name */
	private String outFileName
	
	/** Error console file name */
	private String errFileName
	
	/** Output console stream */
	private PrintStream outStream
	
	/** Error console stream */
	private PrintStream errStream
	
	/**
	 * Redirect output console to file or standard
	 * @param fileName path to console information output file (if null then the standard console is assigned)
	 */
	static void RedirectStdOut(String fileName) {
		global.consistently {
			if (fileName != null) {
				println "Redirect console out to file \"$fileName\""
				FileUtils.ValidFilePath(fileName)
				PrintStream ps = new PrintStream(new BufferedOutputStream(new FileOutputStream(fileName)), true)
				System.setOut(ps)
				global.outFileName = fileName
				global.outStream = ps
			}
			else if (global.outFileName != null) {
				println "Redirect console out to standard ..."
				System.setOut(global.standardConsole)
				global.outFileName = null
				global.outStream = null
			}
		}
	}

	/**
	 * Redirect errors console to file or standard
	 * @param fileName path to console information output file (if null then the standard console is assigned)
	 */
	static void RedirectErrOut(String fileName) {
		global.consistently {
			if (fileName != null) {
				println "Redirect error out to file \"$fileName\""
				FileUtils.ValidFilePath(fileName)
				PrintStream ps = new PrintStream(new BufferedOutputStream(new FileOutputStream(fileName)), true)
				System.setErr(ps)
				global.errFileName = fileName
				global.errStream = ps
			} else if (global.errFileName != null) {
				println "Redirect error out to standard"
				System.setErr(global.errConsole)
				global.errFileName = null
				global.errStream = null
			}
		}
	}

	/** Execute output statements to the log consistently, in the main thread */
	static void Consistently(Closure cl) {
		global.consistently(cl)
	}

	/** Execute output statements to the log consistently, in the main thread */
	void consistently(Closure cl) {
		if (cl != null) {
			synchronized (lockOut) {
				cl.call()
			}
		}
	}

	/**
	 * Write error message to log
	 * @param object repository object
	 * @param message message or code with # prefix
	 * @param vars message variables
	 * @param error cause
	 */
	static Severe(GetlRepository object, String message, Map vars, Throwable error = null) {
		(object.dslCreator?.logging?.manager?:Logs.global).severe(Messages.BuildText(object, message, vars), error)
	}

	/**
	 * Write error message to log
	 * @param object repository object
	 * @param message message or code with # prefix
	 * @param vars message variables
	 * @param error cause
	 */
	static Severe(Getl getl, String message, Map vars, Throwable error = null) {
		(getl.logging?.manager?:Logs.global).severe(Messages.BuildText(message, vars), error)
	}

	/**
	 * Write error message to log
	 * @param object repository object
	 * @param message message or code with # prefix
	 * @param error cause
	 */
	static Severe(GetlRepository object, String message, Throwable error = null) {
		(object.dslCreator?.logging?.manager?:Logs.global).severe(Messages.BuildText(object, message), error)
	}

	/**
	 * Write error message to log
	 * @param object repository object
	 * @param message message or code with # prefix
	 * @param error cause
	 */
	static Severe(Getl getl, String message, Throwable error = null) {
		(getl.logging?.manager?:Logs.global).severe(Messages.BuildText(message), error)
	}

	/**
	 * Write config message to log
	 * @param object repository object
	 * @param message message or code with # prefix
	 * @param vars message variables
	 */
	static Config(GetlRepository object, String message, Map vars = null) {
		(object.dslCreator?.logging?.manager?:Logs.global).config(Messages.BuildText(object, message, vars))
	}

	/**
	 * Write config message to log
	 * @param object repository object
	 * @param message message or code with # prefix
	 * @param vars message variables
	 */
	static Config(Getl getl, String message, Map vars = null) {
		(getl.logging?.manager?:Logs.global).config(Messages.BuildText(message, vars))
	}

	/**
	 * Write warning message to log
	 * @param object repository object
	 * @param message message or code with # prefix
	 * @param vars message variables
	 * @param err exception
	 */
	static Warning(GetlRepository object, String message, Map vars, Throwable err = null) {
		(object.dslCreator?.logging?.manager?:Logs.global).warning(Messages.BuildText(object, message, vars), err)
	}

	/**
	 * Write warning message to log
	 * @param object repository object
	 * @param message message or code with # prefix
	 * @param vars message variables
	 * @param err exception
	 */
	static Warning(Getl getl, String message, Map vars, Throwable err = null) {
		(getl.logging?.manager?:Logs.global).warning(Messages.BuildText(message, vars), err)
	}

	/**
	 * Write warning message to log
	 * @param object repository object
	 * @param message message or code with # prefix
	 * @param err exception
	 */
	static Warning(GetlRepository object, String message, Throwable err = null) {
		(object.dslCreator?.logging?.manager?:Logs.global).warning(Messages.BuildText(object, message), err)
	}

	/**
	 * Write warning message to log
	 * @param object repository object
	 * @param message message or code with # prefix
	 * @param err exception
	 */
	static Warning(Getl getl, String message, Throwable err = null) {
		(getl.logging?.manager?:Logs.global).warning(Messages.BuildText(message), err)
	}

	/**
	 * Write finest message to log
	 * @param object repository object
	 * @param message message or code with # prefix
	 * @param vars message variables
	 */
	static Finest(GetlRepository object, String message, Map vars = null) {
		(object.dslCreator?.logging?.manager?:Logs.global).finest(Messages.BuildText(object, message, vars))
	}

	/**
	 * Write finest message to log
	 * @param object repository object
	 * @param message message or code with # prefix
	 * @param vars message variables
	 */
	static Finest(Getl getl, String message, Map vars = null) {
		(getl.logging?.manager?:Logs.global).finest(Messages.BuildText(message, vars))
	}

	/**
	 * Write finer message to log
	 * @param object repository object
	 * @param message message or code with # prefix
	 * @param vars message variables
	 */
	static Finer(GetlRepository object, String message, Map vars = null) {
		(object.dslCreator?.logging?.manager?:Logs.global).finer(Messages.BuildText(object, message, vars))
	}

	/**
	 * Write finer message to log
	 * @param object repository object
	 * @param message message or code with # prefix
	 * @param vars message variables
	 */
	static Finer(Getl getl, String message, Map vars = null) {
		(getl.logging?.manager?:Logs.global).finer(Messages.BuildText(message, vars))
	}

	/**
	 * Write fine message to log
	 * @param object repository object
	 * @param message message or code with # prefix
	 * @param vars message variables
	 */
	static Fine(GetlRepository object, String message, Map vars = null) {
		(object.dslCreator?.logging?.manager?:Logs.global).fine(Messages.BuildText(object, message, vars))
	}

	/**
	 * Write fine message to log
	 * @param object repository object
	 * @param message message or code with # prefix
	 * @param vars message variables
	 */
	static Fine(Getl getl, String message, Map vars = null) {
		(getl.logging?.manager?:Logs.global).fine(Messages.BuildText(message, vars))
	}

	/**
	 * Write info message to log
	 * @param object repository object
	 * @param message message or code with # prefix
	 * @param vars message variables
	 */
	static Info(GetlRepository object, String message, Map vars = null) {
		(object.dslCreator?.logging?.manager?:Logs.global).info(Messages.BuildText(object, message, vars))
	}

	/**
	 * Write info message to log
	 * @param object repository object
	 * @param message message or code with # prefix
	 * @param vars message variables
	 */
	static Info(Getl getl, String message, Map vars = null) {
		(getl.logging?.manager?:Logs.global).info(Messages.BuildText(message, vars))
	}
}