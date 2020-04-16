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

package getl.lang.opts

import getl.lang.opts.BaseSpec
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