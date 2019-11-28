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

package getl.proc

import getl.exception.ExceptionGETL
import getl.utils.*

/**
 * Base job class
 * @author Alexsey Konstantinov
 *
 */
abstract class Job {
	/**
	 * Job arguments
	 */
	public static final Map jobArgs = [:] as Map<String, Object>
	
	/**
	 * Job arguments (backward compatible) 
	 * @return
	 */
	static Map<String, Object> getArgs() { return jobArgs }
	
	private static void processConfigArgs (def args) {
		Map<String, Object> m = MapUtils.ProcessArguments(args)
		if (m.errout != null) Logs.RedirectErrOut(m.errout as String)
		if (m.stdout != null) {
			Logs.RedirectStdOut(m.stdout as String)
		}
		else if (m.stdcodepage != null) {
			println "Change console code page to \"${m.stdcodepage}\""
			System.setOut(new PrintStream(System.out, true, m.stdcodepage as String))
		}

		// Set job parameters from arguments
		jobArgs.clear()
		jobArgs.putAll(MapUtils.Copy(m, ['stdout', 'stderr', 'stdcodepage']))
		if (jobArgs.vars == null) jobArgs.vars = [:]
		Config.Init(m)
		jobVarsToConfig()
	}

	/**
	 * Copy arguments variable to configuration content
	 */
	static void jobVarsToConfig() {
		// Set variables from arguments
		jobArgs.vars?.each { String key, value ->
			Config.SetValue("vars.$key", value)
		}
	}
	
	/**
	 * Run job with arguments of command line
	 * @param args
	 */
	void run (def args) {
		Config.ClearConfig()
		processConfigArgs(args)
		init()
		Config.LoadConfig()
		doRun()
	}
	
	/**
	 * Place your initialization code here (run after loading configuration files, but before run logic job)
	 */
	void init () {
		
	}
	
	/**
	 * Run vertica without arguments of command line
	 */
	void run () {
		Config.ClearConfig()
		init()
		Config.LoadConfig()
		doRun()
	}
	
	/**
	 * Prepare before run job process
	 */
	protected void prepareRun () { }

	/** Finish job if detected error */
	public static ExitOnError = true
	
	/**
	 * Run job process
	 */
	@SuppressWarnings("UnnecessaryQualifiedReference")
	private void doRun () {
		DateUtils.init()
		getl.deploy.Version.SayInfo()
		prepareRun()
		def isError = false
        Throwable err
		try {
			process()
		}
		catch (Throwable e) {
			Logs.Exception(e, getClass().name, "JOB.RUN")
			isError = true
            err = e
		}
		finally {
			done()
			Logs.Info("### Job stop")
			Logs.Done()
			if (isError && ExitOnError) {
				System.exit(1)
			}
		}
        if (isError) throw err
	}
	
	/**
	 * Place your code before stop job here
	 */
	void done () {
		
	}

	/**
	 * Place your code of process logic here
	 */
	abstract void process()
}