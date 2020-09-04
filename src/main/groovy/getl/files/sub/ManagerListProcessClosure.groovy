package getl.files.sub

import groovy.transform.CompileStatic

/**
 * Processing files by files.Manager.buildList method with closure code
 * @author @author Alexsey Konstantinov
 *
 */
class ManagerListProcessClosure extends ManagerListProcessing {
	/**
	 * Process client code 
	 */
	Closure getCode() { params.code as Closure }
	void setCode(Closure value) { params.code = value }
	
	/**
	 * Run code
	 */
	private Closure runCode

	@Override
    void init() {
		super.init()
		if (code != null) runCode = code.clone() as Closure
	}

	@Override
	@CompileStatic
	Boolean prepare(Map file) {
		if (runCode != null) return runCode.call(file)
		
		true
	}
}
