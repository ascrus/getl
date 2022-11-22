//file:noinspection unused
package getl.proc

import com.fasterxml.jackson.annotation.JsonIgnore
import getl.exception.IncorrectParameterError
import getl.exception.NotSupportError
import getl.lang.Getl
import getl.lang.sub.GetlRepository
import getl.proc.sub.ExecutorFactory
import getl.proc.sub.ExecutorListElement
import getl.proc.sub.ExecutorRunClosure
import getl.proc.sub.ExecutorRunCode
import getl.proc.sub.ExecutorSplitListElement
import getl.proc.sub.ExecutorThread
import getl.proc.sub.ExecutorTimeoutException
import groovy.transform.CompileStatic
import groovy.transform.Synchronized
import groovy.transform.stc.ClosureParams
import groovy.transform.stc.SimpleType
import java.util.concurrent.*
import getl.exception.ExceptionGETL
import getl.utils.*

/**
 * Execution service class
 * @author Alexsey Konstantinov
 *
 */
class Executor implements GetlRepository {
	/** Count thread process */
	private Integer countProc
	/** Count thread process */
	@Synchronized
	Integer getCountProc() { countProc }
	/** Count thread process */
	@Synchronized
	void setCountProc(Integer value) { countProc = value }
	
	/** Limit elements for executed (null or 0-unlimited) */
	private Integer limit
	/** Limit elements for executed (null or 0-unlimited) */
	@Synchronized
	Integer getLimit() { limit }
	/** Limit elements for executed (null or 0-unlimited) */
	@Synchronized
	void setLimit(Integer value) { limit = value }
	
	/** Time waiting for check finish process (default 500ms) */
	private Long waitTime = 500
	/** Time waiting for check finish process (default 500ms) */
	@Synchronized
	Long getWaitTime() { waitTime }
	/** Time waiting for check finish process (default 500ms) */
	@Synchronized
	void setWaitTime(Long value) { waitTime = value }

	/** Abort on error in any process (default false) */
	private Boolean abortOnError = false
	/** Abort on error in any process (default false) */
	@Synchronized
	Boolean getAbortOnError() { abortOnError }
	/** Abort on error in any process (default false) */
	@Synchronized
	void setAbortOnError(Boolean value) { abortOnError = value }
	
	/** Write thread errors to log (default false) */
	private Boolean logErrors = false
	/** Write thread errors to log (default true) */
	@Synchronized
	Boolean getLogErrors() { logErrors }
	/** Write thread errors to log (default false) */
	@Synchronized
	void setLogErrors(Boolean value) { logErrors = value }

	/** Write thread errors to dump file (default false ) */
	private Boolean dumpErrors = false
	/** Write thread errors to dump file (default false ) */
	@Synchronized
	Boolean getDumpErrors() { dumpErrors }
	/** Write thread errors to dump file (default false ) */
	@Synchronized
	void setDumpErrors(Boolean value) { dumpErrors = value }
	
	/** Return element if error (default false) */
	private Boolean debugElementOnError = false
	/** Return element if error (default false) */
	@Synchronized
	Boolean getDebugElementOnError() { debugElementOnError }
	/** Return element if error (default false) */
	@Synchronized
	void setDebugElementOnError(Boolean value) { debugElementOnError = value }

	/** Run has errors */
	private Boolean hasError = false

	/** Threads has errors */
	@Synchronized
	Boolean getIsError () { hasError }

	/** How exceptions in process stopping execute */
	private final Map<Object, Throwable> exceptions = (new HashMap<Object, Throwable>())
	/** How exceptions in process stopping execute */
	@Synchronized
	Map<Object, Throwable> getExceptions() { exceptions }

	/** Fixing error */
	@Synchronized
	protected void setError(Object obj, Throwable except) {
		hasError = true
		if (obj != null)
			exceptions.put(obj, except)
	}
	
	/** List of processing elements */
	private final List list = new CopyOnWriteArrayList(new ArrayList())

	/** List of processing elements */
	List getList() { this.list }
	/** List of processing elements */
	void setList(List value) {
		this.list.clear()
		if (value != null) list.addAll(value)
	}

	/** Use this list for running threads */
	void useList(Object... elements) {
		setList(elements.toList())
	}

	/** Use this list for running threads */
	void useList(List elements) {
		setList(elements)
	}

	/** List closure code for thread run */
	private final List<Closure> listCode = [] as List<Closure>
	/** Adding thread code for run */
	void addThread(Closure cl) { listCode << cl }
	/** Clear current list of registered thread code */
	void clearListThread() { listCode.clear() }

	/** Main code (is executed while running threads) */
	private Closure mainCode
	/** Main code (is executed while running threads) */
	Closure getMainCode() { this.mainCode }
	/** Main code (is executed while running threads) */
	void setMainCode(Closure value) { this.mainCode = value }
	/** Main code (is executed while running threads) */
	void mainCode(Closure value) { setMainCode(value) }

	/** List of all threads */
	private final List<Map> threadList = new LinkedList<Map>()
	/** List of all threads */
	@Synchronized
	List<Map> getThreadList() { threadList }

	/** List of active threads */
	private final List<Map> threadActive = new LinkedList<Map>()
	/** List of active threads */
	@Synchronized
	List<Map> getThreadActive() { threadActive }
	/** Add node to active threads list */
	@Synchronized
	private void addNodeToThreadActive(Map node) {
		threadActive.add(node)
	}
	/** Remove node from active threads list */
	@Synchronized
	private void removeNodeToThreadActive(Map node) {
		threadActive.remove(node)
	}
	/** Cancel active threads */
	@Synchronized
	private void cancelActiveThreads() {
		threadActive.each { Map serv ->
			(serv.threadSubmit as Future)?.cancel(true)
		}
	}

	/** Interrupt flag */
	private Boolean isInterrupt = false
	//private final Object lockIsInterrupt = new Object()

	/** Interrupt flag */
	@Synchronized
	Boolean getIsInterrupt() { isInterrupt }

	/** Interrupt flag */
	@Synchronized
	void setIsInterrupt(Boolean value) { isInterrupt = value }

	/**
	 * Launches a single code
	 * @param code
	 */
	void runSingle(Closure code) {
		run([1], 1, code)
	}
	
	/**
	 * 
	 * @param count
	 * @param code
	 */
	void runMany(Integer countList, @ClosureParams(value = SimpleType, options = ['java.lang.Integer']) Closure code) {
		def l = (1..countList)
		run(l, countList, code)
	}

	void runMany(Integer countList, Integer countThread,
				 @ClosureParams(value = SimpleType, options = ['java.lang.Integer']) Closure code) {
		def l = (1..countList)
		run(l, countThread, code)
	}

	/** Code on dispose resource after run the thread */
	private final List<Closure> listDisposeThreadResource = [] as List<Closure>
	/** Added code on dispose resource after run the thread */
	void disposeThreadResource(Closure cl) { listDisposeThreadResource.add(cl) }

	/** Checking element permission */
	private Closure<Boolean> onValidAllowRun
	/** Checking element permission */
	Closure<Boolean> getOnValidAllowRun() { onValidAllowRun }
	/** Checking element permission */
	void setOnValidAllowRun(Closure<Boolean> value) { onValidAllowRun = value }
	/** Checking element permission */
	void validAllowRun(Closure<Boolean> value) { setOnValidAllowRun(value) }

	/** Run initialization code when starting a thread */
	private Closure onStartingThread
	/** Run initialization code when starting a thread */
	Closure getOnStartingThread() { onStartingThread }
	/** Run initialization code when starting a thread */
	void setOnStartingThread(Closure value) { onStartingThread = value }
	/** Run initialization code when starting a thread */
	void startingThread(@ClosureParams(value = SimpleType, options = ['java.util.HashMap']) Closure value) {
		setOnStartingThread(value)
	}

	/** Run finalization code when stopping a thread */
	private Closure onFinishingThread
	/** Run finalization code when stopping a thread */
	Closure getOnFinishingThread() { onFinishingThread }
	/** Run finalization code when stopping a thread */
	void setOnFinishingThread(Closure value) { onFinishingThread = value }
	/** Run finalization code when stopping a thread */
	void finishingThread(@ClosureParams(value = SimpleType, options = ['java.util.HashMap']) Closure value) {
		setOnFinishingThread(value)
	}

	/** Component runs threads */
	private Boolean isRunThreads = false
	/** Component runs threads */
	@Synchronized
	Boolean getRunThreads() { isRunThreads }
	@Synchronized
	void setRunThreads(Boolean value) { isRunThreads = value }

	/** Run thread code with list elements */
	void run(Integer countThread, Closure code) {
		run(list, countThread, code)
	}

	/** Run thread code with list elements */
	void runWithElements(@ClosureParams(value = SimpleType, options = ['getl.proc.sub.ExecutorListElement']) Closure cl) {
		if (!(list instanceof List<ExecutorListElement>))
			throw new ExceptionGETL('Requires List<ExecutorListElement> type for list elements!')
		run(null, null, cl)
	}


	/** Run thread code with list elements */
	void runWithElements(Integer countThread, @ClosureParams(value = SimpleType, options = ['getl.proc.sub.ExecutorListElement']) Closure cl) {
		if (!(list instanceof List<ExecutorListElement>))
			throw new ExceptionGETL('Requires List<ExecutorListElement> type for list elements!')
		run(null, countThread, cl)
	}

	/** Execution pool */
	private ExecutorService threadPool

	/**
	 * Run code in threads over list items
	 * @param elements list of processed elements (the default is specified in the "list")
	 * @param countThread number of threads running simultaneously
	 * @param code list item processing code
	 */
	void run(List elements = list, Integer countThread = countProc, Closure code) {
		if (runThreads)
			throw new ExceptionGETL('Cannot start "run" method when threads are running!')

		hasError = false
		isInterrupt = false
		exceptions.clear()
		threadList.clear()
		threadActive.clear()
		counter.clear()
		counterProcessed.clear()

		if (elements == null) elements = list

		if (elements == null || elements.isEmpty())
			throw new ExceptionGETL("List of items to process is empty!")

		if (countThread == null) countThread = countProc?:elements?.size()

		def runCode = { Map node ->
			def num = (node.num as Integer) + 1
			def element = node.element
			node.put('start',  new Date())

			addNodeToThreadActive(node)

			try {
				if ((!isError || !abortOnError) && !isInterrupt) {
					def allowRun = true
					if (onValidAllowRun != null) {
						allowRun = onValidAllowRun.call(element)
					}
					if (allowRun) {
						try {
							code.call(element)
							counterProcessed.nextCount()
						}
						catch (Throwable e) {
							if (abortOnError)
								throw e
						}
					}
				}
			}
			catch (Throwable e) {
				//noinspection GroovyVariableNotAssigned
				processRunError(e, node, num, element, threadList)
			}
			finally {
				node.put('finish', new Date())
				try {
					if (Thread.currentThread() instanceof ExecutorThread)
						(Thread.currentThread() as ExecutorThread).clearCloneObjects(listDisposeThreadResource, logger)
				}
				finally {
					removeNodeToThreadActive(node)
					node.remove('threadSubmit')
					(node.element as List).clear()
					node.remove('element')
				}
			}
		}

		threadPool = (countThread > 1)?Executors.newFixedThreadPool(countThread, new ExecutorFactory()):Executors.newSingleThreadExecutor(new ExecutorFactory())
		runThreads = true
		try {
			def size = elements.size()
			if (limit?:0 > 0 && limit < size) size = limit
			for (Integer i = 0; i < size; i++) {
				def r = new HashMap<String, Object>()
				r.num = i
				r.element = elements[i]
				r.threadSubmit = (threadPool.submit({ -> (runCode.clone() as Closure).call(r) } as Callable) as Future)
				threadList << r
			}
			threadPool.shutdown()

			addShutdownHook {
				if (threadPool != null && !threadPool.isShutdown())
					threadPool.shutdownNow()
			}

			while (!threadPool.terminated) {
				if (mainCode != null && !isInterrupt && (!abortOnError || !isError)) {
					try {
						callSynch(mainCode)
					}
					catch (Throwable e) {
						setError(null, e)
						cancelActiveThreads()
						threadPool.shutdownNow()
						throw e
					}
				}
				threadPool.awaitTermination(waitTime, TimeUnit.MILLISECONDS)
			}

			if (isError && abortOnError) {
				def objects = []
				def num = 0
				exceptions.each { obj, Throwable e ->
					num++
					def errorText = "  <${e.getClass().name}> " + e.message
					if (debugElementOnError) {
						objects.add("[$num] $errorText: ${obj.toString()}")
						logger.dump(e, "${getClass().name} [$num]", obj.toString(), null)
					} else {
						objects.add("[$num] $errorText")
					}
				}
				throw new ExceptionGETL("Thread errors:\n${objects.join('\n')}")
			}

			if (mainCode != null && !isInterrupt && (!abortOnError || !isError))
				callSynch(mainCode)
		}
		finally {
			exceptions.clear()
			threadList.clear()
			threadActive.clear()
			threadPool = null

			runThreads = false
		}
	}

	/**
	 * Run code in threads over list items by dividing a list between threads into sub lists
	 * @param elements list of processed elements (the default is specified in the "list")
	 * @param countThread number of threads running simultaneously
	 * @param code list item processing code
	 */
	@SuppressWarnings(['DuplicatedCode'])
	void runSplit(List elements = list, Integer countThread = countProc,
				  @ClosureParams(value = SimpleType, options = ['getl.proc.sub.ExecutorSplitListElement']) Closure code) {
		if (runThreads)
			throw new ExceptionGETL('Cannot start "runSplit" method when threads are running!')

		hasError = false
		isInterrupt = false
		exceptions.clear()
		threadList.clear()
		threadActive.clear()
		counter.clear()
		counterProcessed.clear()

		if (elements == null) elements = list

		if (elements == null || elements.isEmpty())
			throw new ExceptionGETL("List of items to process is empty!")

		if (countThread == null) countThread = countProc?:elements?.size()

		def runCode = { Map node ->
			def num = node.num
			def threadList = (node.element as List)
			node.put('start',  new Date())
			def curElement

			addNodeToThreadActive(node)

			try {
				if (onStartingThread)
					onStartingThread.call(node)

				def inf = new ExecutorSplitListElement(node: node)

				threadList.each { element ->
					if (!((!isError || !abortOnError) && !isInterrupt)) {
						//noinspection UnnecessaryQualifiedReference
						directive = Closure.DONE
						return
					}

					curElement = element

					def allowRun = true
					if (onValidAllowRun != null) {
						allowRun = onValidAllowRun.call(element)
					}
					if (allowRun) {
						try {
							inf.item = element
							code.call(inf)
							counterProcessed.nextCount()
						}
						catch (Throwable e) {
							if (abortOnError)
								throw e
						}
					}
				}
			}
			catch (Throwable e) {
				//noinspection GroovyVariableNotAssigned
				processRunError(e, node, num, curElement, threadList)
			}
			finally {
				node.put('finish', new Date())
				try {
					if (onFinishingThread != null)
						onFinishingThread.call(node)
				}
				finally {
					if (Thread.currentThread() instanceof ExecutorThread)
						(Thread.currentThread() as ExecutorThread).clearCloneObjects(listDisposeThreadResource, logger)
				}
				removeNodeToThreadActive(node)
				node.remove('threadSubmit')
				(node.element as List).clear()
				node.remove('element')
			}
		}

		def listElements = ListUtils.SplitList(elements, countThread, ((limit?:0 > 0)?limit:null))
		threadPool = (countThread > 1)?Executors.newFixedThreadPool(countThread, new ExecutorFactory()):Executors.newSingleThreadExecutor(new ExecutorFactory())
		runThreads = true
		try {
			for (Integer i = 0; i < listElements.size(); i++) {
				def r = new HashMap<String, Object>()
				r.num = i
				r.element = listElements[i]
				r.threadSubmit = (threadPool.submit({ -> (runCode.clone() as Closure).call(r) } as Callable) as Future)
				threadList << r
			}
			threadPool.shutdown()

			addShutdownHook {
				if (threadPool != null && !threadPool.isShutdown())
					threadPool.shutdownNow()
			}

			while (!threadPool.terminated) {
				if (mainCode != null && !isInterrupt && (!abortOnError || !isError)) {
					try {
						callSynch(mainCode)
					}
					catch (Throwable e) {
						setError(null, e)
						cancelActiveThreads()
						threadPool.shutdownNow()
						throw e
					}
				}
				threadPool.awaitTermination(waitTime, TimeUnit.MILLISECONDS)
			}

			if (isError && abortOnError) {
				def objects = []
				def num = 0
				exceptions.each { obj, Throwable e ->
					num++
					def errorText = "  <${e.getClass().name}> " + e.message
					if (debugElementOnError) {
						objects << "[$num] $errorText: ${obj.toString()}"
						logger.dump(e, "${getClass().name} [$num]", obj.toString(), null)
					} else {
						objects << "[$num] $errorText"
					}
				}
				throw new ExceptionGETL("Thread errors:\n${objects.join('\n')}")
			}

			if (mainCode != null && !isInterrupt && (!abortOnError || !isError))
				callSynch(mainCode)
		}
		finally {
			exceptions.clear()
			threadList.clear()
			threadActive.clear()

			runThreads = false
		}
	}

	/**
	 * Run code in threads over list items by dividing a list between threads into sub lists
	 * @param elements list of processed elements (the default is specified in the "list")
	 * @param countThread number of threads running simultaneously
	 * @param code list item processing code
	 */
	void runSplit(Integer countThread, Closure code) {
		runSplit(list, countThread, code)
	}

	@Synchronized
	private void processRunError(Throwable e, Map node, Object num, Object element, List elements) {
		try {
			threadActive.remove(node)
			threadPool?.shutdownNow()

			node.putAll([threadSubmit: null, error: e])
			setError(element, e)
			def errObject = (debugElementOnError)?"[${num}]: ${element}":"Element ${num}"
			if (dumpErrors)
				logger.dump(e, getClass().name, errObject, "LIST: ${MapUtils.ToJson([data: element])}")
			if (logErrors) {
				logger.exception(e, this.toString(), errObject)
			}
		}
		catch (Throwable ignored) { }
	}

	/** Run thread code with list elements */
	void exec(Integer countThread) {
		exec(listCode, countThread)
	}

	/** Run thread code with list elements */
	void exec(List<Closure> elements = listCode, Integer countThread = countProc) {
		if (runThreads)
			throw new ExceptionGETL('Cannot start "exec" method when threads are running!')

		hasError = false
		isInterrupt = false
		exceptions.clear()
		threadList.clear()
		threadActive.clear()
		counter.clear()
		counterProcessed.clear()

		if (countThread == null) countThread = countProc?:elements.size()

		def runCode = { Map node ->
			def num = (node.num as Integer) + 1
			def element = node.element as Closure
			node.put('start',  new Date())
			try {
				if (limit?:0 == 0 || num <= limit) {
					if ((!isError || !abortOnError) && !isInterrupt) {
						addNodeToThreadActive(node)
						try {
							element.call()
							counterProcessed.nextCount()
						}
						finally {
							if (Thread.currentThread() instanceof ExecutorThread)
								(Thread.currentThread() as ExecutorThread).clearCloneObjects(listDisposeThreadResource, logger)
						}
						removeNodeToThreadActive(node)
						node.remove('threadSubmit')
						node.remove('element')
					}
				}
			}
			catch (Throwable e) {
				processRunError(e, node, num, element, elements)
			}
			finally {
				node.put('finish', new Date())
			}
		}

		threadPool = (countThread > 1)?Executors.newFixedThreadPool(countThread, new ExecutorFactory()):Executors.newSingleThreadExecutor(new ExecutorFactory())
		runThreads = true
		try {
			def num = 0
			elements.each { n ->
				Map r = Collections.synchronizedMap(new HashMap())
				r.num = num
				r.element = n
				r.threadSubmit = threadPool.submit({ -> runCode.call(r) } as Callable)
				threadList << r

				num++
			}
			threadPool.shutdown()

			addShutdownHook {
				if (threadPool != null && !threadPool.isShutdown())
					threadPool.shutdownNow()
			}

			while (!threadPool.isTerminated()) {
				if (mainCode != null && !isInterrupt && (!abortOnError || !isError)) {
					try {
						callSynch(mainCode)
					}
					catch (Throwable e) {
						setError(null, e)
						cancelActiveThreads()
						threadPool.shutdownNow()
						throw e
					}
				}
				threadPool.awaitTermination(waitTime, TimeUnit.MILLISECONDS)
			}

			if (isError && abortOnError) {
				def objects = []
				num = 0
				exceptions.each { obj, Throwable e ->
					num++
					def errorText = "  <${e.getClass().name}> " + e.message
					if (debugElementOnError) {
						objects << "[$num] $errorText: ${obj.toString()}"
						logger.dump(e, "${getClass().name} [$num]", obj.toString(), null)
					} else {
						objects << "[$num] $errorText"
					}
				}
				throw new ExceptionGETL("Executer has errors for run on objects:\n${objects.join('\n')}")
			}

			if (mainCode != null && !isInterrupt && (!abortOnError || !isError))
				callSynch(mainCode)
		}
		finally {
			exceptions.clear()
			threadList.clear()
			threadActive.clear()
			threadPool = null

			runThreads = false
		}
	}

	private ExecutorService threadBackground
	private Boolean runBackgroundService = false
	
	@Synchronized
	Boolean isRunBackground () { runBackgroundService }
	
	/** Start background process */
	void startBackground(Closure code) {
		if (runThreads)
			throw new ExceptionGETL('Cannot start "startBackground" when threads are running!')

		def runCode = {
			try {
				while (isRunBackground()) {
					code.call()
					sleep waitTime
				}
			}
			catch (Throwable e) {
				if (logErrors)
					logger.exception(e, this.toString(), null)
			}
		}
		
		runBackgroundService = true
		runThreads = true
		try {
			threadBackground = Executors.newSingleThreadExecutor()
			threadBackground.execute(runCode)
		}
		catch (Throwable e) {
			runBackgroundService = false
			runThreads = false
			throw e
		}

		if (isRunBackground()) {
			addShutdownHook {
				if (isRunBackground())
					stopBackground()
			}
		}
	}
	
	/** Finish background process */
	@Synchronized
	void stopBackground () {
		if (!isRunBackground())
			throw new ExceptionGETL("Not Background process running")
		try {
			runBackgroundService = false
			if (threadBackground != null) {
				try {
					threadBackground.shutdown()
					while (!threadBackground.isShutdown())
						threadBackground.awaitTermination(waitTime, TimeUnit.MILLISECONDS)
				}
				finally {
					threadBackground = null
				}
			}
		}
		finally {
			runThreads = false
		}
	}
	/** Run code with ignore runtime errors */
	static Boolean RunIgnoreErrors (Closure code) {
		RunIgnoreErrors(null, code)
	}

	/** Run code with ignore runtime errors */
	static Boolean RunIgnoreErrors (Getl owner, Closure code) {
		try {
			code.call()
		} 
		catch (Throwable e) {
			((owner?.logging?.manager != null)?owner.logging.manager:Logs.global).finer(StringUtils.FormatException("Ignore error", e))
			return false 
		}
		
		return true
	}

	@Synchronized
	@SuppressWarnings("GrMethodMayBeStatic")
	void callSynch(Closure cl) {
		cl.call()
	}

	/** Synchronized counter for work between threads */
	private final def counter = new SynchronizeObject()
	/** Synchronized counter for work between threads */
	SynchronizeObject getCounter() { counter }

	/** The number of successfully processed list items in threads */
	private final SynchronizeObject counterProcessed = new SynchronizeObject()
	/** The number of successfully processed list items in threads */
	Long getCountProcessed() { counterProcessed.count }

	private String _dslNameObject
	@JsonIgnore
	@Override
	String getDslNameObject() { _dslNameObject }
	@Override
	void setDslNameObject(String value) { _dslNameObject = value }

	private Getl _dslCreator
	@JsonIgnore
	@Override
	Getl getDslCreator() { _dslCreator }
	@Override
	void setDslCreator(Getl value) { _dslCreator = value }

	private Date _dslRegistrationTime
	@JsonIgnore
	@Override
	Date getDslRegistrationTime() { _dslRegistrationTime }
	@Override
	void setDslRegistrationTime(Date value) { _dslRegistrationTime = value }

	@Override
	void dslCleanProps() {
		_dslNameObject = null
		_dslCreator = null
		_dslRegistrationTime = null
	}

	/** Current logger */
	@JsonIgnore
	Logs getLogger() { (dslCreator?.logging?.manager != null)?dslCreator.logging.manager:Logs.global }

	/**
	 * Execute code no longer than the specified time interval
	 * @param maximumProcessingTime maximum allowed execution time in milliseconds
	 * @param cl executable code
	 * @return code was executed within the specified time interval
	 */
	@CompileStatic
	static void runClosureWithTimeout(Long maximumProcessingTime, Closure cl) {
		if (cl == null)
			throw new NullPointerException('Execution code not set in parameter "cl"!')

		if (maximumProcessingTime == null || maximumProcessingTime == 0) {
			cl.call()
			return
		}

		runCodeWithTimeout(maximumProcessingTime, new ExecutorRunClosure(cl))
	}

	/**
	 * Execute code no longer than the specified time interval
	 * @param maximumProcessingTime maximum allowed execution time in milliseconds
	 * @param code executable code
	 * @return code was executed within the specified time interval
	 */
	@CompileStatic
	static void runCodeWithTimeout(Long maximumProcessingTime, ExecutorRunCode code) {
		if (code == null)
			throw new NullPointerException('Execution code not set in parameter "code"!')

		if (maximumProcessingTime == null || maximumProcessingTime == 0) {
			try {
				code.code()
			}
			catch (Throwable e) {
				try {
					code.error(e)
				}
				catch (Throwable ignored) { }
				throw e
			}
			return
		}

		if (maximumProcessingTime < 0)
			throw new IncorrectParameterError('#params.great_zero', 'maximumProcessingTime', 'Executor.runCodeWithTimeout')

		def ownerThread = (Thread.currentThread() instanceof ExecutorThread)?Thread.currentThread() as ExecutorThread:null as ExecutorThread
		def service = Executors.newSingleThreadExecutor(new ExecutorFactory(ownerThread))
		service.submit(code)
		service.shutdown()

		def res = true
		try {
			if (!service.awaitTermination(maximumProcessingTime, TimeUnit.MILLISECONDS)) {
				service.shutdownNow()
				res = false
			}
		}
		catch (InterruptedException e) {
			service.shutdownNow()
			throw e
		}

		if (code.runCodeError != null)
			throw code.runCodeError

		if (!res)
			throw new ExecutorTimeoutException(code.timeoutMessageError())
	}

	@Override
	Object clone() {
		throw new NotSupportError('clone')
	}
}