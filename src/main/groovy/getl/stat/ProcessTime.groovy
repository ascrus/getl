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

package getl.stat

import java.util.logging.Level
import groovy.time.*
import getl.utils.*

/**
 * Time statistic manager class
 * @author Alexsey Konstantinov
 *
 */
@SuppressWarnings("UnnecessaryQualifiedReference")
class ProcessTime {
	public static java.util.logging.Level LogLevelDefault = Level.FINER

    static void SetLogLevelDefault (String level) { LogLevelDefault = Logs.StrToLevel(level) }
	public static boolean debugDefault = false
	
	public String name = 'process'
	public Level logLevel
	public String objectName = 'row'
	public String abbrName = '<STAT>'
	public boolean debug = debugDefault
	
	public Date start
	public Date finish
	public TimeDuration time
	public Long countRow
	public long rowInSec
	public BigDecimal avgSpeed
	public String avgSpeedStr

    /**
     * Init function
     */
	private void init () {
		start = DateUtils.Now()
		
		def conf = Config.FindSection("statistic")
		if (conf != null) {
			if (conf.level != null && logLevel == null) logLevel = Logs.StrToLevel(conf.level as String)
			if (conf.debug != null) debug = conf.debug 
		}
		if (logLevel == null) logLevel = LogLevelDefault
		
		if (logLevel != Level.OFF && debug) {
			def msg = "$abbrName START ${name}"
			Logs.Write(logLevel, msg)
		}
	}

    /**
     * Constructor
     */
	ProcessTime () {
		init()
	}

    /**
     * Constructor
     * @param params
     */
	ProcessTime(Map params) {
		if (params.name != null) {
			this.name = params.name
		}
		else
		if (params.className != null) {
			this.name = params.className
		}
		
		if (params.logLevel != null) {
			logLevel = Logs.StrToLevel(params.logLevel as String)
		}
		
		if (params.objectName != null) {
			objectName = params.objectName 
		}
		
		if (params.debug != null) {
			debug = params.debug
		}

		if (params.abbrName != null) {
			abbrName = params.abbrName
		}
		
		init()
	}

    /**
     * Start process time
     */
	Date getStart() { start }

    /**
     * Finish process time
     */
	Date getFinish() { finish }

    /**
     * Duration time process
     */
	TimeDuration getTime() { time }

    /**
     * Total rows processed
     */

	long GetCountRow() { countRow }

    /**
     * Count rows processed by second
     */
	long getRowInSec() { rowInSec }

    /**
     * Finish process
     */
	void finish() {
		this.finish((Long)null)
	}

    /**
     * Finish process
     * @param procRow
     */
	void finish(Long procRow) {
		this.finish = DateUtils.Now()
		this.time = TimeCategory.minus(finish, start)
		this.countRow = procRow
		if (this.countRow != null) {
			this.rowInSec = (this.time.toMilliseconds() / 1000 > 0)?this.countRow / (this.time.toMilliseconds() / 1000):this.countRow
			this.avgSpeed = (this.countRow > 0)?(this.time.toMilliseconds() / 1000) / this.countRow:0
			this.avgSpeedStr = String.format("%20.6f", this.avgSpeed).trim().replace(",", ".")
		}

		if (logLevel != Level.OFF) {
			def msg = "${name}: ${lastStat()}".toString()
			if (debug) msg = "$abbrName FINISH " + msg else msg = "$abbrName " + msg
			Logs.Write(logLevel, msg)
		}
	}

    /**
     * Return last status process
     */
	String lastStat () {
		def res
		if (objectName?.toLowerCase() != 'byte')
			res = "time ${time}" + ((countRow!= null)?", $countRow ${objectName}s, $rowInSec ${objectName}s per second, ${avgSpeedStr} seconds per $objectName":"")
		else
			res = "time ${time}" + ((countRow!= null)?", ${FileUtils.sizeBytes(countRow)}, ${FileUtils.avgSpeed(countRow, time.toMilliseconds())}":"")

		return res
	}
}