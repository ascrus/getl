package getl.proc.sub

import groovy.transform.InheritConstructors

import java.util.concurrent.ThreadFactory

/**
 * Thread factory class for execution service
 * @author Alexsey Konstantinov
 */
@InheritConstructors
class ExecutorFactory implements ThreadFactory {
    ExecutorFactory() {
        super()
    }

    ExecutorFactory(ExecutorThread owner) {
        super()
        this._owner = owner
    }

    private ExecutorThread _owner

    @Override
    Thread newThread(Runnable r) {
        def res = new ExecutorThread(r)
        if (_owner != null)
            res.ownerThread = _owner

        return res
    }
}