package getl.exception

import groovy.transform.CompileStatic

/**
 * Generated when calling to stop DSL code execution by method abortWithError
 */
@CompileStatic
class AbortDsl extends Exception {
    AbortDsl() {
        super()
    }

    AbortDsl(Integer typeCode, Integer exitCode, String message) {
        super(message)
        this.typeCode = typeCode
        this.exitCode = exitCode
    }

    AbortDsl(Integer typeCode, Integer exitCode) {
        super()
        this.typeCode = typeCode
        this.exitCode = exitCode
    }

    AbortDsl(Integer typeCode, String message) {
        super(message)
        this.typeCode = typeCode
    }

    AbortDsl(Integer typeCode) {
        super()
        this.typeCode = typeCode
    }

    AbortDsl(String message) {
        super(message)
        this.typeCode = 0
        this.exitCode = -1
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