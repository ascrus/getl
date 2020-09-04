package getl.proc.sub

import groovy.transform.InheritConstructors

import java.util.concurrent.ThreadFactory

/**
 * Thread factory class for execution service
 * @author Alexsey Konstantinov
 */
@InheritConstructors
class ExecutorFactory implements ThreadFactory {
    @Override
    Thread newThread(Runnable r) {
        return new ExecutorThread(r)
    }
}