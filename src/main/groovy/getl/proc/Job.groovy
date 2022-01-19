package getl.proc

import com.fasterxml.jackson.annotation.JsonIgnore
import getl.lang.Getl
import getl.utils.*

/**
 * Base job class
 * @author Alexsey Konstantinov
 *
 */
abstract class Job {
	/** Getl owner */
	protected Getl dslCreator

	/** Current logger */
	@JsonIgnore
	Logs getLogger() { (dslCreator?.logging?.manager != null)?dslCreator.logging.manager:Logs.global }

	/** Job arguments */
	static public final Map jobArgs = new HashMap<String, Object>()
	
	/**
	 * Job arguments (backward compatible) 
	 * @return
	 */
	static Map<String, Object> getArgs() { return jobArgs }
	
	static private void processConfigArgs (def args) {
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
		//noinspection SpellCheckingInspection
		jobArgs.putAll(MapUtils.Copy(m, ['stdout', 'stderr', 'stdcodepage']))
		if (jobArgs.vars == null)
			jobArgs.vars = new HashMap()
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
	 * @param args command arguments
	 */
	void run(def args = null) {
		//Config.ClearConfig()
		if (args != null)
			processConfigArgs(args)
		init()
		if (dslCreator == null)
			Config.LoadConfig()
		doRun()
	}
	
	/** Place your initialization code here (run after loading configuration files, but before run logic job) */
	void init() { }
	
	/**
	 * Prepare before run job process
	 */
	protected void prepareRun() { }

	/** Finish job if detected error */
	static public Boolean ExitOnError = true
	
	/**
	 * Run job process
	 */
	@SuppressWarnings(["UnnecessaryQualifiedReference", "GroovyVariableNotAssigned"])
	protected void doRun () {
		DateUtils.init()
		//if (dslCreator == null)
		Logs.Init()
		getl.deploy.Version.SayInfo()
		prepareRun()
		def isError = false
        Throwable err
		try {
			process()
		}
		catch (Throwable e) {
			logger.exception(e, getClass().name, "JOB.RUN")
			isError = true
            err = e
		}
		finally {
			done()
			logger.info("### Job stop${(exitCode != null)?" with code $exitCode":''}")
			logger.done()
			if (isError && ExitOnError) {
				System.exit(exitCode?:1)
			}
		}

        if (isError)
			throw err
	}

	/** Exit application code */
	private Integer exitCode
	/** Exit application code */
	Integer getExitCode() { exitCode }
	protected void setExitCode(Integer value) { exitCode = value }
	
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