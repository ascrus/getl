//file:noinspection unused
package getl.models.sub

import getl.lang.sub.ScriptEvents
import getl.models.Workflows
import getl.models.opts.WorkflowScriptSpec
import getl.models.opts.WorkflowSpec
import getl.utils.sub.BaseUserCode
import groovy.transform.CompileStatic

/**
 * Base class for extending workflow with custom code
 * @author Alexsey Konstantinov
 */
@CompileStatic
class WorkflowUserCode extends BaseUserCode {
    /** Current workflow model */
    public Workflows currentModel

    /** Workflow startup parameters */
    Map<String, Object> getArgs() { (currentModel.modelVars + scriptExtendedVars) as Map<String, Object> }

    /** Set variable value in workflow script */
    Map<String, Object> vars(String scriptName) {
        return currentModel.vars(scriptName)
    }

    /** Return result from workflow script */
    Map result(String scriptName) {
        return currentModel.result(scriptName)
    }

    /** Workflow model variables */
    Map<String, Object> getModelVars() { currentModel.modelVars }

    /** Workflow model attributes */
    Map<String, Object> getModelAttrs() { currentModel.modelAttrs }

    /** Events on script */
    ScriptEvents events(String scriptName, @DelegatesTo(ScriptEvents) Closure cl = null) {
        return currentModel.events(scriptName, cl)
    }

    /** List of script in model */
    List<String> getModelScripts() { currentModel.listScripts().keySet().toList() }

    /** Model step by name */
    WorkflowSpec step(String stepName) { currentModel.stepByName(stepName) }

    /** Model script by name */
    WorkflowScriptSpec script(String scriptName) { currentModel.scriptByName(scriptName) }
}