package getl.proc

import getl.utils.*

/**
 * Base job class
 * @author Alexsey Konstantinov
 *
 */
abstract class Job {
	/** Job arguments */
	static public final Map jobArgs = [:] as Map<String, Object>
	
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
	static public Boolean ExitOnError = true
	
	/**
	 * Run job process
	 */
	@SuppressWarnings(["UnnecessaryQualifiedReference", "GroovyVariableNotAssigned"])
	protected void doRun () {
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
			Logs.Info("### Job stop${(exitCode != null)?" with code $exitCode":''}")
			Logs.Done()
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