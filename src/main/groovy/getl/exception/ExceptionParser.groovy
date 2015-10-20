package getl.exception

import groovy.transform.InheritConstructors

@InheritConstructors
class ExceptionParser extends Exception {
	/**
	 * Tokens from parsing
	 */
	public List<Map> tokens
	
	ExceptionParser (String message, List<Map> tokens) {
		super(message)
		this.tokens = tokens
	}
}
