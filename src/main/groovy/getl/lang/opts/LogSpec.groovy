package getl.lang.opts

import getl.utils.*
import groovy.transform.InheritConstructors

import java.util.logging.*

/**
 * Log specification class
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
class LogSpec extends BaseSpec {
    /**
     * Current logger
     */
    Logger getLogLogger() { Logs.logger  }

    /**
     * Current formatter
     */
    Logs.LogFormatter getLogFormatter() { Logs.formatter }

    /**
     * Print configuration message
     */
    Boolean getLogPrintConfigMessage() { Logs.printConfigMessage }

    /**
     * Print configuration message
     */
    void setLogPrintConfigMessage(Boolean value) { Logs.printConfigMessage = value }

    /**
     * Current file name handler
     */
    String getLogFileNameHandler() { Logs.fileNameHandler }

    /**
     * Current file handler
     */
    FileHandler getLogFileHandler() { Logs.file }

    /**
     * Current file handler
     */
    void setLogFileHandler(FileHandler value) { Logs.file = value }

    /**
     * Current log file name
     */
    String getLogFileName() { Logs.logFileName }

    /**
     * Current log file name
     */
    void setLogFileName(String value) { Logs.logFileName = value; Logs.Init() }

    /**
     * Print stack trace for error
     */
    Boolean getLogPrintStackTraceError() { Logs.printStackTraceError }

    /**
     * Print stack trace for error
     */
    Boolean setLogPrintStackTraceError(Boolean value) { Logs.printStackTraceError = value }

    /** Convert string value level to type */
    Level strToLevel(String level) { Logs.StrToLevel(level) }

    /** Redirect standart output to specified file */
    void redirectStdOut(String fileName) { Logs.RedirectStdOut(fileName) }

    /** Redirect errors output to specified file */
    void redirectErrorsOut(String fileName) { Logs.RedirectErrOut(fileName) }
}