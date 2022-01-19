package getl.models.opts

import getl.exception.ExceptionModel
import getl.lang.opts.BaseSpec
import groovy.transform.InheritConstructors

/**
 * Workflow script options
 * @author Alexsey Konstantinov
 */
@InheritConstructors
class WorkflowScriptSpec extends BaseSpec {
    /** Owner workflow step options */
    WorkflowSpec getOwnerWorkflowSpec() { ownerObject as WorkflowSpec }

    @Override
    protected void initSpec() {
        super.initSpec()
        if (params.vars == null)
            params.vars = new HashMap()
    }

    /** The class name of the script to run */
    String getClassName() { params.className as String }
    /** The class name of the script to run */
    void setClassName(String value) {
        if (value == null)
            throw new ExceptionModel("The class name of the script being executed in step " +
                    "\"${ownerWorkflowSpec.stepName}\" is required!")

        params.className = value
    }

    /** Variables of the script to run */
    Map getVars() { params.vars as Map }
    /** Variables of the script to run */
    void setVars(Map value) {
        vars.clear()
        if (value != null)
            vars.putAll(value)
    }
}