package getl.proc.sub

import groovy.transform.CompileStatic

/**
 * Class for executing code on timeout
 * @author Alexsey Konstantinov
 */
@CompileStatic
abstract class ExecutorRunCode extends ExecutorThread {
    private Throwable runCodeError
    /** Error while executing code */
    Throwable getRunCodeError() { runCodeError }

    /** Execution code */
    abstract void code()

    @Override
    void run() {
        try {
            code()
        }
        catch (Throwable e) {
            runCodeError = e
            try {
                error(e)
            }
            catch (Throwable ignored) { }
        }
    }

    /** Handling runtime error */
    void error(Throwable e) { }

    /** Timeout error text */
    String timeoutMessageError() { 'Exceeding the allowed execution time!' }
}