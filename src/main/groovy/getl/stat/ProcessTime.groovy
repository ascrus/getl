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
	ProcessTime () {
		init()
	}

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

	static public java.util.logging.Level LogLevelDefault = Level.FINER
    static void SetLogLevelDefault (String level) { LogLevelDefault = Logs.StrToLevel(level) }

	static public Boolean debugDefault = false

	/** Process name */
	public String name = 'process'
	/** Level profile logging */
	public Level logLevel
	/** Metric */
	public String objectName = 'row'
	/** Abbreviate */
	public String abbrName = '<STAT>'
	/** Debugging */
	public Boolean debug = debugDefault

	/** Start time */
	public Date start
	/** Finish time */
	public Date finish
	/** Duration */
	public TimeDuration time
	/** Processed rows */
	public Long countRow
	/** Process rows in second */
	public Long rowInSec
	/** Average rows processed */
	public BigDecimal avgSpeed
	/** Average rows processed */
	public String avgSpeedStr

    /** Init function */
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

    /** Finish process */
	void finish() {
		this.finish(null as Long)
	}

    /** Finish process */
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

    /** Return last status process */
	String lastStat () {
		def res
		if (objectName?.toLowerCase() != 'byte')
			res = "time ${time}" + ((countRow!= null)?", ${StringUtils.WithGroupSeparator(countRow)} ${objectName}s, " +
					"${StringUtils.WithGroupSeparator(rowInSec)} ${objectName}s per second, " +
					"$avgSpeedStr seconds per $objectName":"")
		else
			res = "time ${time}" + ((countRow!= null)?", ${FileUtils.SizeBytes(countRow)}, ${FileUtils.AvgSpeed(countRow, time.toMilliseconds())}":"")

		return res
	}
}