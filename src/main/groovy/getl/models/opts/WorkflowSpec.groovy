//file:noinspection unused
package getl.models.opts

import getl.exception.ExceptionModel
import getl.lang.sub.ParseObjectName
import getl.models.Workflows
import getl.models.sub.BaseSpec
import groovy.transform.InheritConstructors
import groovy.transform.stc.ClosureParams
import groovy.transform.stc.SimpleType

/**
 * Workflow options
 * @author Alexsey Konstantinov
 */
@InheritConstructors
class WorkflowSpec extends BaseSpec {
    @SuppressWarnings('SpellCheckingInspection')
    static enum Operation { EXECUTE, ERROR }

    /** Process start operator */
    static public final Operation executeOperation = Operation.EXECUTE
    /** Process error operator */
    static public final Operation errorOperation = Operation.ERROR

    WorkflowSpec(Workflows owner, String stepName, Operation operation) {
        super(owner)

        setStepName(stepName.trim())
        setOperation(operation)
    }

    /** Owner workflow */
    Workflows getOwnerWorkflow() { ownerModel as Workflows }

    @Override
    protected void initSpec() {
        super.initSpec()
        if (params.scripts == null)
            params.scripts = [:] as Map<String, Map<String, Object>>
        if (params.nested == null)
            params.nested = [] as List<WorkflowSpec>
    }

    @Override
    protected List<String> ignoreImportKeys(Map<String, Object> importParams) { ['nested'] }

    @Override
    void importFromMap(Map<String, Object> importParams) {
        super.importFromMap(importParams)
        def nestList = importParams.nested as List<Map>
        def list = [] as List<WorkflowSpec>
        nestList?.each { node ->
            list.add(new WorkflowSpec(ownerWorkflow, node))
        }
        nested = list
    }

    @Override
    protected String objectNameInModel() { stepName }

    /** Step name */
    String getStepName() { params.stepName as String }
    /** Step name */
    void setStepName(String value) {
        if (value == null)
            throw new ExceptionModel('Required step name!')

        if (!ParseObjectName.CheckNameCharacters(value))
            throw new ExceptionModel('Step name \"$value\" contains invalid characters!')

        saveParamValue('stepName', value)
    }

    /** Execute operation */
    Operation getOperation() { params.operation as Operation }
    /** Execute operation */
    void setOperation(Operation value) {
        if (value == null)
            throw new ExceptionModel('Required step operation!')

        saveParamValue('operation', value)
    }

    /** Step condition code */
    String getCondition() { params.condition as String }
    /** Step condition code */
    void setCondition(String value) { saveParamValue('condition', value) }

    /** Step initialization code */
    String getInitCode() { params.initCode as String }
    /** Step initialization code */
    void setInitCode(String value) { saveParamValue('initCode', value) }

    /** Step finalization code */
    String getFinalCode() { params.finalCode as String }
    /** Step finalization code */
    void setFinalCode(String value) { saveParamValue('finalCode', value) }

    /** Number of simultaneously executed script (default 1) */
    Integer getCountThreads() { params.countThreads as Integer }
    /** Number of simultaneously executed script (default 1) */
    void setCountThreads(Integer value) {
        if ((value?:0) < 1)
            throw new ExceptionModel('The number of simultaneously executed scripts cannot be less than 1!')

        saveParamValue('countThreads', value)
    }

    /** List of scripts for processing */
    Map<String, Map<String, Object>> getScripts() { params.scripts as Map<String, Map<String, Object>> }
    /** List of scripts for processing */
    void setScripts(Map<String, Map<String, Object>> value) {
        scripts.clear()

        if (value != null)
            scripts.putAll(value)
    }

    /**
     * Add script to step
     * @param name script name in workflow
     * @return script description
     */
    WorkflowScriptSpec exec(String name,
                            @DelegatesTo(WorkflowScriptSpec)
                            @ClosureParams(value = SimpleType, options = ['getl.models.opts.WorkflowScriptSpec'])
                                    Closure cl = null) {
        if (name == null || name.length() == 0)
            throw new ExceptionModel("The script name in step \"$stepName\" is required!")

        if (!ParseObjectName.CheckNameCharacters(name))
            throw new ExceptionModel('Script name \"$name\" contains invalid characters!')

        def exists = ownerWorkflow.scriptByName(name)
        if (exists != null)
            throw new ExceptionModel("The script named \"$name\" is already defined in the " +
                    "\"${exists.ownerWorkflowSpec.stepName}\" step!")

        def scriptParams = [:] as Map<String, Object>
        scriptParams.vars = [:] as Map<String, Object>

        def parent = new WorkflowScriptSpec(this, true, scriptParams)
        parent.runClosure(cl)
        scripts.put(name, scriptParams)

        return parent
    }
    /**
     * Add script to step
     * @param runClass the class to run
     * @param scriptParams script parameters
     */
    void exec(@DelegatesTo(WorkflowScriptSpec)
                @ClosureParams(value = SimpleType, options = ['getl.models.opts.WorkflowScriptSpec'])
                        Closure cl = null) {
        exec(ownerWorkflow.defaultScriptName(), cl)
    }

    /** Nested steps */
    List<WorkflowSpec> getNested() { params.nested as List<WorkflowSpec> }
    /** Nested steps */
    void setNested(List<WorkflowSpec> value) {
        nested.clear()
        if (value != null)
            nested.addAll(value)
    }

    /**
     * Define nested step
     * @param stepName operation step name
     * @param operation execute operation
     * @param cl defining code
     * @return step specification
     */
    protected WorkflowSpec addStep(String stepName, Operation operation,
                                   @DelegatesTo(WorkflowSpec)
                                   @ClosureParams(value = SimpleType, options = ['getl.models.opts.WorkflowSpec'])
                                           Closure cl = null) {
        if (stepName == null || stepName.length() == 0)
            throw new ExceptionModel('Step name required!')

        if (ownerWorkflow.stepByName(stepName) != null)
            throw new ExceptionModel("The step named \"$stepName\" is already defined in the workflow!")

        if (operation == Operation.ERROR && nested.find { node -> node.operation == Operation.ERROR } != null)
            throw new ExceptionModel('Only one "error" operation is supported per workflow step!')

        def parent = new WorkflowSpec(ownerWorkflow, stepName, operation)
        nested.add(parent)
        parent.runClosure(cl)

        return parent
    }

    /** Find script */
    Map<String, Object> findScript(String scriptName) {
        if (scriptName == null || scriptName.length() == 0)
            throw new ExceptionModel("Required script name for find script function!")

        scriptName = scriptName.toUpperCase()
        return scripts.find { it.key.toUpperCase() == scriptName }.value
    }

    /**
     * Define step in workflow
     * @param stepName operation step name
     * @param cl defining code
     * @return step specification
     */
    WorkflowSpec later(String stepName,
                       @DelegatesTo(WorkflowSpec)
                       @ClosureParams(value = SimpleType, options = ['getl.models.opts.WorkflowSpec'])
                               Closure cl = null) {
        addStep(stepName, Operation.EXECUTE, cl)
    }

    /**
     * Define nested step
     * @param cl defining code
     * @return step specification
     */
    WorkflowSpec later(@DelegatesTo(WorkflowSpec)
                      @ClosureParams(value = SimpleType, options = ['getl.models.opts.WorkflowSpec'])
                              Closure cl = null) {
        addStep(ownerWorkflow.defaultStepName(), Operation.EXECUTE, cl)
    }

    /**
     * Define nested error processing
     * @param stepName operation step name
     * @param cl defining code
     * @return step specification
     */
    WorkflowSpec onError(String stepName,
                         @DelegatesTo(WorkflowSpec)
                         @ClosureParams(value = SimpleType, options = ['getl.models.opts.WorkflowSpec'])
                                 Closure cl = null) {
        addStep(stepName, errorOperation, cl)
    }

    /**
     * Define nested error processing
     * @param cl defining code
     * @return step specification
     */
    WorkflowSpec onError(@DelegatesTo(WorkflowSpec)
                         @ClosureParams(value = SimpleType, options = ['getl.models.opts.WorkflowSpec'])
                              Closure cl = null) {
        addStep(ownerWorkflow.defaultStepName(), Operation.ERROR, cl)
    }

    @Override
    String toString() { "$stepName" }
}