package getl.proc.sub

import groovy.transform.CompileStatic

/**
 * Class for executing closure on timeout
 * @author Alexsey Konstantinov
 */
@CompileStatic
class ExecutorRunClosure extends ExecutorRunCode {
    ExecutorRunClosure(Closure cl) {
        super()

        if (cl == null)
            throw new NullPointerException('Need closure!')

        this.cl = cl
    }

    private Closure cl

    @Override
    void code() {
        cl.call()
    }
}