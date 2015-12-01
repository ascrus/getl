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

package getl.proc

import java.util.concurrent.Callable
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import groovy.transform.Synchronized
import getl.exception.ExceptionGETL
import getl.utils.*

/**
 * Thread execution manager class
 * @author Alexsey Konstantinov
 *
 */
class Executor {
	/**
	 * Count thread process
	 */
	public int countProc = 1
	
	/**
	 * Limit elements for executed (0-unlimited)
	 */
	public int limit = 0
	
	/**
	 * Time waiting for check finish process 
	 */
	public long waitTime = 500
	
	/**
	 * Abort on error in any process
	 */
	public boolean abortOnError = false
	
	/**
	 * Write thread errors to log 
	 */
	public boolean logErrors = true
	
	/**
	 * Return element if error
	 */
	public boolean debugElementOnError = false
	
	/**
	 * Allow multi-threaded execution
	 */
	public boolean allowThread = true
	
	/**
	 * Run has errors
	 */
	private boolean hasError = false
	
	/**
	 * Run has errors
	 * @return
	 */
	@Synchronized
	public boolean getIsError () { hasError }
	
	/**
	 * How exceptions in process stopping execute
	 */
	public final Map exceptions = [:]
	
	@Synchronized
	protected void setError (def obj, def except) {
		hasError = true
		if (obj != null) exceptions.put(obj, except)
	}
	
	/**
	 * List of processes
	 */
	public def list = []
	
	/**
	 * Main code (is executed while running threads)
	 */
	public Closure mainCode
	
	private boolean isInterrupt = false

	/**
	 * Set interrupt flag for current thread processes	
	 * @param value
	 */
	@Synchronized
	public void setInterrupt(boolean value) { isInterrupt = true }   
	
	/**
	 * Start processing the specified code list items
	 * @param code
	 */
	public void run(Closure code) {
		run(list, countProc, code)
	}
	
	/**
	 * Launches a single code
	 * @param code
	 */
	public void runSingle(Closure code) {
		run([1], 2, code)
	}
	
	/**
	 * 
	 * @param count
	 * @param code
	 */
	public void runMany(int countList, Closure code) {
		def l = (1..countList)
		run(l, countList, code)
	}
	
	public void runMany(int countList, int countProc, Closure code) {
		def l = (1..countList)
		run(l, countProc, code)
	}
	
	public void run(List list, int countProc, Closure code) {
		hasError = false
		isInterrupt = false
		exceptions.clear()
		
		def runCode = { num, element -> 
			try {
				if (limit == 0 || num <= limit) {
					if ((!isError || !abortOnError) && !isInterrupt) code(element)
				}
			}
			catch (Throwable e) {
				try {
//					org.codehaus.groovy.runtime.StackTraceUtils.sanitize(e)
					setError(element, e)
					def errObject = (debugElementOnError)?"[${num}]: ${element}":"Element ${num}"
					Logs.Dump(e, getClass().name, errObject, "LIST: ${MapUtils.ToJson([list: list])}")
					if (logErrors) {
						Logs.Exception(e, this.toString(), errObject)
						e.printStackTrace()
					}
				}
				catch (Throwable ee) { }
			}
		}
		
		if (allowThread && countProc > 1) {
			def threadPool = Executors.newFixedThreadPool(countProc)
			def threadSubmit = []
			def num = 0
			list.each { n ->
				num++
				def t = threadPool.submit({ -> runCode(num, n)} as Callable)
				threadSubmit << t
			} 
			threadPool.shutdown()
			
			while (!threadPool.isTerminated()) {
				if (mainCode != null && !isInterrupt && (!abortOnError || !isError)) {
					try {
						mainCode()
					}
					catch (Throwable e) {
						setError(null, e)
						threadSubmit.each { it.cancel(true) }
						threadPool.shutdownNow()
						throw e
					}
				}
				threadPool.awaitTermination(waitTime, TimeUnit.MILLISECONDS)
			}
		}
		else {
			def num = 0
			list.each {
				num++ 
				if (!isInterrupt && (!isError || !abortOnError)) runCode(num, it) 
			}
		}
		
		if (isError && abortOnError) {
			def objects = []
			def num = 0
			exceptions.each { obj, Throwable e ->
				num++
				if (debugElementOnError) {
					objects << "[${num}] ${e.message}: ${obj.toString()}"
				}
				else {
					objects << "[${num}] ${e.message}"
				}
				
				/*
				Logs.Dump(e, getClass().name, elem, "LIST: ${ListUtils.ToJson(list)}")
				
				org.codehaus.groovy.runtime.StackTraceUtils.sanitize(e)
				e.printStackTrace()
				*/
			}
			throw new ExceptionGETL("Executer has errors for run on objects:\n${objects.join('\n')}")
		}
		
		if (mainCode != null && !isInterrupt && (!abortOnError || !isError)) mainCode()
	}
	
	private ExecutorService threadBackground
	private boolean  runBackground = false
	
	/**
	 * Start background process
	 * @param code
	 */
	public void startBackground(Closure code) {
		if (runBackground) throw new ExceptionGETL("Background process already running")
		def runCode = {
			try {
				while (runBackground) {
					code()
					sleep waitTime
				}
			}
			catch (Throwable e) {
				if (logErrors) {
					Logs.Exception(e, this.toString(), null)
					org.codehaus.groovy.runtime.StackTraceUtils.sanitize(e)
					e.printStackTrace()
				}
			}
		}
		
		runBackground = true
		threadBackground = Executors.newSingleThreadExecutor()
		threadBackground.execute(runCode)
	}
	
	/**
	 * Finish background process
	 */
	public void stopBackground () {
		if (!runBackground) throw new ExceptionGETL("Not Background process running")
		runBackground = false
		try {
			threadBackground.shutdown()
			while (!threadBackground.isShutdown()) threadBackground.awaitTermination(waitTime, TimeUnit.MILLISECONDS)
		}
		finally {
			threadBackground = null
		}
	}
	
	public static boolean RunIgnoreErrors (Closure code) {
		try {
			code()
		} 
		catch (Throwable e) { 
			Logs.Finest("Ignore error: ${e.message}")
			return false 
		}
		
		return true
	}
}
