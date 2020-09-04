package getl.exception

/**
 * Exception SQL parser
 * @author Alexsey Konstantinov
 *
 */
class ExceptionParser extends ExceptionGETL {
	/**
	 * Tokens from parsing
	 */
	public List<Map> tokens
	
	ExceptionParser (String message, List<Map> tokens) {
		super(message)
		this.tokens = tokens
	}
}
