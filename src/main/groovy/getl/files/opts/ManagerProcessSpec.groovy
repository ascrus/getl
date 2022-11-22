package getl.files.opts

import getl.exception.RequiredParameterError
import getl.files.Manager
import getl.lang.opts.BaseSpec

/**
 * Options and result of execution of launched processes
 * @author Alexsey Konstantinov
 */
class ManagerProcessSpec extends BaseSpec {
    ManagerProcessSpec(Manager owner) {
        super(owner)
        if (owner == null)
            throw new RequiredParameterError('owner')

        this.manager = owner
    }

    /** File system manager */
    private Manager manager

    /** Console output */
    private final List<String> console = [] as List<String>
    /** Console output */
    List<String> getConsole() { console }

    /** Errors output */
    private final List<String> errors = [] as List<String>
    /** Errors output */
    List<String> getErrors() { errors }

    /** Result code */
    private final List<Integer> result = [] as List<Integer>
    /** Result code */
    List<Integer> getResult() { result }

    /** Last result code */
    private Integer lastResult
    /** Last result code */
    Integer getLastResult() {
        if (result.isEmpty()) return null
        return result[result.size() - 1]
    }

    /** Last console output */
    String getLastConsole() {
        if (console.isEmpty()) return null
        return console[console.size() - 1]
    }

    /** Last errors output */
    String getLastErrors() {
        if (errors.isEmpty()) return null
        return errors[console.size() - 1]
    }

    /**
     * Run process
     * @param command process start command
     */
    Integer run(String command) {
        def out = new StringBuilder()
        def err = new StringBuilder()

        def res = manager.command(command, out, err)
        result.add(res)
        console.add(out.toString())
        errors.add(err.toString())

        return res
    }
}