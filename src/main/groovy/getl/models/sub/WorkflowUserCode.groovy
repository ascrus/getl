package getl.models.sub

import getl.exception.ExceptionModel
import getl.lang.Getl
import getl.models.Workflows

/**
 * Base class for extending workflow with custom code
 * @author Alexsey Konstantinov
 */
class WorkflowUserCode extends Getl {
    private Map<String, Map<String, Object>> scriptVars = new HashMap<String, Map<String, Object>>()

    /** Current workflow model */
    public Workflows currentModel

    /** Workflow startup parameters */
    Map<String, Object> getArgs() { (currentModel.modelVars + scriptExtendedVars) as Map<String, Object> }

    /** Set variable value in workflow script */
    Map<String, Object> vars(String scriptName) {
        if (scriptName == null || scriptName.length() == 0)
            throw new ExceptionModel('The script name is required for "vars" function!')

        if (currentModel.scriptByName(scriptName) == null)
            throw new ExceptionModel("There is script \"$scriptName\" specified in the vars function, which is not defined " +
                    "for model \"${currentModel.dslNameObject}\"!")

        scriptName = scriptName.toUpperCase()
        def sv = scriptVars.get(scriptName)
        if (sv == null) {
            sv = new HashMap<String, Object>()
            scriptVars.put(scriptName, sv)
        }
        return sv
    }

    /** Return result from workflow script */
    Map result(String scriptName) {
        return currentModel.result(scriptName)
    }

    /** Workflow model variables */
    Map<String, Object> getModelVars() { currentModel.modelVars }
}