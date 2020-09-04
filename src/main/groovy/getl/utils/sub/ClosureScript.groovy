package getl.utils.sub

import groovy.transform.CompileStatic

/**
 * Сonvert closure to script object
 * @author Alexsey Konstantonov
 *
 */
@CompileStatic
class ClosureScript extends Script {
    Closure closure

    @Override
    Object run() {
        closure.resolveStrategy = Closure.DELEGATE_FIRST
        closure.delegate = this
        closure.call()
    }
}
