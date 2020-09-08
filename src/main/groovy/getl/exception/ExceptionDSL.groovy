package getl.exception

/**
 * DSL GETL exception
 * @author Alexsey Konstantinov
 */
class ExceptionDSL extends Throwable {
    ExceptionDSL(Integer typeCode, Integer exitCode, String message) {
        super(message)
        this.typeCode = typeCode
        this.exitCode = exitCode
    }

    ExceptionDSL(Integer typeCode, Integer exitCode) {
        super()
        this.typeCode = typeCode
        this.exitCode = exitCode
    }

    ExceptionDSL(Integer typeCode, String message) {
        super(message)
        this.typeCode = typeCode
    }

    ExceptionDSL(Integer typeCode) {
        super()
        this.typeCode = typeCode
    }

    ExceptionDSL(String message) {
        super(message)
        this.typeCode = 0
        this.exitCode = -1
    }

    ExceptionDSL() {
        super()
    }

    /** Stop code execution of the current class */
    static public final Integer STOP_CLASS = 1
    /**
     * Stop execution of current application code
     */
    static public final Integer STOP_APP = 2

    /** Type code */
    private Integer typeCode
    /** Type code */
    Integer getTypeCode() { typeCode }

    /** Exit code */
    private Integer exitCode
    /** Exit code */
    Integer getExitCode() { exitCode }
}