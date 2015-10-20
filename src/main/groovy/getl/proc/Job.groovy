package getl.proc

/**
 GETL - based package in Groovy, which automates the work of loading and transforming data. His name is an acronym for «Groovy ETL».

 GETL is a set of libraries of pre-built classes and objects that can be used to solve problems unpacking,
 transform and load data into programs written in Groovy, or Java, as well as from any software that supports
 the work with Java classes.
 
 Copyright (C) 2013  Alexsey Konstantonov (ASCRUS)

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

import getl.exception.ExceptionGETL
import getl.utils.*

/**
 * Base job class
 * @author Alexsey Konstantinov
 *
 */
abstract class Job {
	private void processConfigArgs (def args) {
		def m = MapUtils.ProcessArguments(args)
		if (m.errout != null) Logs.RedirectErrOut(m.errout)
		if (m.stdout != null) {
			Logs.RedirectStdOut(m.stdout)
		}
		else if (m.stdcodepage != null) {
			println "Change console code page to \"${m.stdcodepage}\""
			System.setOut(new PrintStream(System.out, true, m.stdcodepage))
		}

		// Set job parameters from arguments
		if (m.config != null) {
			Map config = m.config 
			if (config.path != null) {
				Config.path = config.path
				if (!(new File(Config.path).exists())) throw new ExceptionGETL("Can not find config path \"${Config.path}\"")
			}
			def configPath = (Config.path != null)?"${Config.path}${File.separator}":""
			if (config.filename != null) {
				def fn = config.filename
				if (fn.indexOf(";") == -1) {
					Config.fileName = fn
					if (!(new File(configPath + Config.fileName).exists())) throw new ExceptionGETL("Can not find config file \"${Config.fileName}\"")
				}
				else {
					def fs = fn.split(";")
					fs.each {
						if (!(new File(configPath + it).exists())) throw new ExceptionGETL("Can not find config file \"${it}\"")
					}
					Config.files.addAll(fs)
				}
			}
		}
		Config.SetValue("job.args", MapUtils.Copy(m, ['stdout', 'stderr', 'stdcodepage']))
		
		// Set variables from arguments
		Config.content.job?.args?.vars?.each { key, value ->
			Config.SetValue("vars.$key", value)
		}
	}
	
	/**
	 * Job command line arguments
	 * @return
	 */
	public Map getArgs() { Config.content.job.args }
	
	/**
	 * Run job with arguments of command line
	 * @param args
	 */
	public void run (def args) {
		processConfigArgs(args)
		
		init()
		Config.LoadConfig()
		doRun()
	}
	
	/**
	 * Run job with class name configuration and arguments of command line
	 * @param jobClass
	 * @param codePage
	 * @param args
	 */
	public void run (Class jobClass, String codePage, def args) {
		processConfigArgs(args)
		
		init()
		Config.LoadConfigClass(jobClass, codePage)
		doRun()
	}
	
	/**
	 * Place your initialization code here (run after loading configuration files, but before run logic job)
	 */
	public void init () {
		
	}
	
	/**
	 * Run jobs without arguments of command line
	 */
	public void run () {
		init()
		Config.LoadConfig()
		doRun()
	}
	
	/**
	 * Prepare before run job process
	 */
	protected void prepareRun () { }
	
	/**
	 * Run job process
	 */
	private void doRun () {
		Logs.Init()
		Logs.Fine("### GETL / version ${getl.deploy.Version.version} created by ${getl.deploy.Version.years} / All right reserved for EasyData company")
		Logs.Info("### Job start")
		prepareRun()
		def isError = false
		try {
			process()
		}
		catch (Throwable e) {
			Logs.Exception(e, getClass().name, "JOB.RUN")
			isError = true
//			org.codehaus.groovy.runtime.StackTraceUtils.sanitize(e)
//			e.printStackTrace()
		}
		finally {
			done()
			Logs.Info("### Job stop")
			Logs.Done()
			if (isError) {
				System.exit(1)
			}
		}
	}
	
	/**
	 * Place your code before stop job here
	 */
	public void done () {
		
	}

	/**
	 * Place your code of process logic here
	 */
	abstract void process()
}
