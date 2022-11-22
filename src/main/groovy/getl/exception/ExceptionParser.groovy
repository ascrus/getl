package getl.exception

import groovy.transform.CompileStatic

/**
 * Exception SQL parser
 * @author Alexsey Konstantinov
 *
 */
@CompileStatic
class ExceptionParser extends ExceptionGETL {
	/** Tokens from parsing */
	public List<Map> tokens
	
	ExceptionParser (String message, List<Map> tokens) {
		super(message)
		this.tokens = tokens
	}
}
