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
	
	private void init () {
		start = new Date()
		
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
	
	ProcessTime () {
		init()
	}
	
	ProcessTime(Map params) {
		if (params.name != null) {
			this.name = params.name
		}
		else
		if (params.className != null) {
			this.name = params.className.name
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
	
	public Date getStart() { start }
	public Date getFinish() { finish }
	public TimeDuration getTime() { time }
	public long GetCountRow() { countRow }
	public long getRowInSec() { rowInSec }
	
	public void finish(Long procRow) {
		finish = new Date()
		time = TimeCategory.minus(finish, start)
		countRow = procRow
		if (countRow != null) {
			rowInSec = (time.toMilliseconds() / 1000 > 0)?countRow / (time.toMilliseconds() / 1000):countRow
			avgSpeed = (countRow > 0)?(time.toMilliseconds() / 1000) / countRow:0
			avgSpeedStr = String.format("%20.6f", avgSpeed).trim().replace(",", ".")
		}
		

		if (logLevel != Level.OFF) {
			def msg = "${name}: ${lastStat()}"
			if (debug) msg = "<STAT FINISH> " + msg else msg = "<STAT> " + msg  
			Logs.Write(logLevel, msg)
		}
	}
	
	public String lastStat () {
		"time ${time}" + ((countRow!= null)?", $countRow ${objectName}s, $rowInSec ${objectName}s per second, ${avgSpeedStr} seconds per $objectName":"")
	}
	
	public void finish() {
		finish(null)
	}
}
