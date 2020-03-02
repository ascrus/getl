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

import getl.csv.CSVDataset
import getl.data.*
import getl.exception.ExceptionGETL
import getl.jdbc.JDBCDataset
import getl.jdbc.TableDataset
import getl.utils.BoolUtils
import groovy.transform.InheritConstructors
import java.util.logging.Level

/**
 * Getl language options
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
class LangSpec extends BaseSpec {
    /** Fixing the execution time of processes in the log */
    boolean getProcessTimeTracing() { BoolUtils.IsValue(params.processTimeTracing, true) }
     /** Fixing the execution time of processes in the log */
    void setProcessTimeTracing(boolean value) {params.processTimeTracing = value }

    /** Log process profiling start time */
    boolean getProcessTimeDebug() { BoolUtils.IsValue(params.processTimeDebug, false) }
    /** Log process profiling start time */
    void setProcessTimeDebug(boolean value) { params.processTimeDebug = value }

    /** The level of fixation in the log of process profiling records */
    Level getProcessTimeLevelLog() { (params.processTimeLevelLog as Level)?:Level.FINER }
    /** The level of fixation in the log of process profiling records */
    void setProcessTimeLevelLog(Level value) { params.processTimeLevelLog = value }

    /** Use the multithreaded JDBC connection model */
    boolean getUseThreadModelConnection() { BoolUtils.IsValue(params.useThreadModelJDBCConnection, true) }
    /** Use the multithreaded JDBC connection model */
    void setUseThreadModelConnection(boolean value) { params.useThreadModelJDBCConnection = value }

    /** Write SQL command from temporary database connection to history file */
    String getTempDBSQLHistoryFile() { params.tempDBSQLHistoryFile as String }
    /** Write SQL command from temporary database connection to history file */
    void setTempDBSQLHistoryFile(String value) { params.tempDBSQLHistoryFile = value }

    /** Auto create CSV temp dataset for JDBC tables */
    Boolean getAutoCSVTempForJDBDTables() { BoolUtils.IsValue(params.autoCSVTempForJDBDTables) }
    /** Auto create CSV temp dataset for JDBC tables */
    void setAutoCSVTempForJDBDTables(Boolean value) { params.autoCSVTempForJDBDTables = value }

    /** Check on connection registration */
    Boolean getValidRegisterObjects() { BoolUtils.IsValue(params.validObjectExist, true) }
    /** Check on connection registration */
    void setValidRegisterObjects(Boolean value) { params.validObjectExist = value }

    /**  Process management dataset */
    Dataset getProcessControlDataset() { params.processControlDataset as Dataset }
    /**  Process management dataset */
    void setProcessControlDataset(Dataset value) {
        if (value != null) {
            if (!(value instanceof TableDataset || value instanceof CSVDataset))
                throw new ExceptionGETL('To control the operation of processes, a dataset with types "table" or "csv" can be used!')

            def name = value.fieldByName('name')
            if (name == null)
                throw new ExceptionGETL('Required field "name"!')
            if (name.type != Field.stringFieldType)
                throw new ExceptionGETL('Field "name" must be of string type"!')

            def enabled = value.fieldByName('enabled')
            if (enabled == null)
                throw new ExceptionGETL('Required field "enabled"!')
            if (!(enabled.type in [Field.stringFieldType, Field.integerFieldType, Field.bigintFieldType, Field.booleanFieldType]))
                throw new ExceptionGETL('Field "name" must be of string, integer or boolean type"!')
        }

        params.processControlDataset = value
    }

    /** Check permission to work processes when they start */
    Boolean getCheckProcessOnStart() { BoolUtils.IsValue(params.checkProcessOnStart) }
    /** Check permission to work processes when they start */
    void setCheckProcessOnStart(Boolean value) { params.checkProcessOnStart = value }

    /** Check permission to work processes when they start */
    Boolean getCheckProcessForThreads() { BoolUtils.IsValue(params.checkProcessForThreads) }
    /** Check permission to work processes when they start */
    void setCheckProcessForThreads(Boolean value) { params.checkProcessForThreads = value }

    /** The default output level of the echo command to the log for sql object */
    Level getSqlEchoLogLevel() { (params.sqlEchoLogLevel as Level)?:processTimeLevelLog }
    /** The default output level of the echo command to the log for sql object */
    void setSqlEchoLogLevel(Level value) { params.sqlEchoLogLevel = value }
}