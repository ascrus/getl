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
class ProcessTime {
	public static java.util.logging.Level LogLevelDefault = Level.FINE
	public static void SetLogLevelDefault (String level) { LogLevelDefault = Logs.StrToLevel(level) }
	public static boolean debugDefault = false
	
	public String name = "process"
	public Level logLevel
	public String objectName = "row"
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
			def msg = "<STAT START> ${name}"
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
		
		init()
	}

    /**
     * Start process time
     */
	public Date getStart() { start }

    /**
     * Finish process time
     */
	public Date getFinish() { finish }

    /**
     * Duration time process
     */
	public TimeDuration getTime() { time }

    /**
     * Total rows processed
     */

	public long GetCountRow() { countRow }

    /**
     * Count rows processed by second
     */
	public long getRowInSec() { rowInSec }

    /**
     * Finish process
     */
	public void finish() {
		this.finish((Long)null)
	}

    /**
     * Finish process
     * @param procRow
     */
	public void finish(Long procRow) {
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
			if (debug) msg = "<STAT FINISH> " + msg else msg = "<STAT> " + msg  
			Logs.Write(logLevel, msg)
		}
	}

    /**
     * Return last status process
     */
	public String lastStat () {
		"time ${time}" + ((countRow!= null)?", $countRow ${objectName}s, $rowInSec ${objectName}s per second, ${avgSpeedStr} seconds per $objectName":"")
	}
}
