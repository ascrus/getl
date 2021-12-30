package getl.lang.opts

import com.fasterxml.jackson.annotation.JsonIgnore
import getl.exception.ExceptionDSL
import getl.utils.*
import groovy.transform.InheritConstructors
import java.util.logging.*

/**
 * Log specification class
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
@SuppressWarnings(['GrMethodMayBeStatic', 'unused'])
class LogSpec extends BaseSpec {
    @Override
    protected void initSpec() {
        super.initSpec()
        if (params.logManager == null) {
            def manager = new Logs(StringUtils.RandomStr())
            saveParamValue('logManager', manager)
        }
    }

    /** Current log manager */
    @JsonIgnore
    Logs getManager() { params.logManager as Logs }

    /** Current Java logger */
    @JsonIgnore
    Logger getLogLogger() { manager.logger  }

    /** Current formatter */
    @JsonIgnore
    Logs.LogFormatter getLogFormatter() { manager.formatter }

    /** Print configuration message */
    Boolean getLogPrintConfigMessage() { manager.printConfigMessage }
    /** Print configuration message */
    void setLogPrintConfigMessage(Boolean value) { manager.printConfigMessage = value }

    /** Current file name handler */
    @JsonIgnore
    String getLogFileNameHandler() { manager.fileNameHandler }

    /** Current file handler */
    @JsonIgnore
    FileHandler getLogFileHandler() { manager.file }

    /** Log file name */
    String getLogFileName() { manager.logFileName }
    /** Log file name */
    void setLogFileName(String value) { manager.logFileName = value }

    /** The level of message logging to a file (default INFO) */
    Level getLogFileLevel() { manager.logFileLevel }
    /** The level of message logging to a file (default INFO) */
    void setLogFileLevel(Level value) { manager.logFileLevel = value }

    /** The level of message logging to a file (default FINEST) */
    Level getLogConsoleLevel() { manager.logConsoleLevel }
    /** The level of message logging to a file (default FINEST) */
    void setLogConsoleLevel(Level value) { manager.logConsoleLevel = value }

    /** Print stack trace for error */
    Boolean getLogPrintStackTraceError() { manager.printStackTraceError }
    /** Print stack trace for error */
    Boolean setLogPrintStackTraceError(Boolean value) { manager.printStackTraceError = value }

    /** The default output level of the echo command to the log for sql object */
    Level getSqlEchoLogLevel() { (params.sqlEchoLogLevel as Level)?:Level.FINE }
    /** The default output level of the echo command to the log for sql object */
    void setSqlEchoLogLevel(Level value) { saveParamValue('sqlEchoLogLevel', value) }

    /** Convert string value level to type */
    Level strToLevel(String level) { Logs.StrToLevel(level) }

    /** Redirect standard output to specified file */
    void redirectStdOut(String fileName) { Logs.RedirectStdOut(fileName) }

    /** Redirect errors output to specified file */
    void redirectErrorsOut(String fileName) { Logs.RedirectErrOut(fileName) }

    /**
     * Attach the value of a variable to the filename
     * @param varValue value of a variable
     */
    void attachToFileName(String varValue) {
        if (logFileName == null)
            throw new ExceptionDSL("The file name for the log is not set!")
        if (varValue == null)
            throw new ExceptionDSL("A value is required for the attached variable to the log name!")
        def ext = FileUtils.ExtensionWithoutFilename(logFileName)
        if ((ext?:'') != '')
            logFileName = FileUtils.FilenameWithoutExtension(logFileName) + '.' + varValue + '.' + ext
        else
            logFileName = FileUtils.FilenameWithoutExtension(logFileName) + '.' + varValue
    }

    /**
     * Write error trace to dump log file
     * @param error error exception
     * @param typeObject object type name
     * @param nameObject object name
     * @param data additional error data
     */
    void dump(Throwable error, String typeObject, String nameObject, def data) {
        manager.dump(error, typeObject, nameObject, data)
    }
}