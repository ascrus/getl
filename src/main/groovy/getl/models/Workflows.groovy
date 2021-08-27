//file:noinspection unused
package getl.models

import getl.exception.ExceptionModel
import getl.models.opts.WorkflowSpec
import getl.models.opts.WorkflowSpec.Operation
import getl.models.sub.BaseModel
import getl.models.sub.BaseSpec
import getl.proc.Executor
import getl.utils.CloneUtils
import groovy.transform.InheritConstructors
import groovy.transform.stc.ClosureParams
import groovy.transform.stc.SimpleType

/**
 * Description of the steps in the workflow
 * @author Alexsey Konstantinov
 */
@InheritConstructors
class Workflows extends BaseModel<WorkflowSpec> {
    /** List of used steps */
    List<WorkflowSpec> getUsedSteps() { usedObjects as List<WorkflowSpec> }
    /** List of used steps */
    void setUsedSteps(List<WorkflowSpec> value) {
        usedSteps.clear()
        if (value != null)
            usedSteps.addAll(value)
    }
    /** Convert a list of parameters to usable workflow steps */
    void assignUsedSteps(List<Map> value) {
        def own = this
        def list = [] as List<WorkflowSpec>
        value?.each { node ->
            def p = CloneUtils.CloneMap(node, true)
            list.add(new WorkflowSpec(own, p))
        }
        usedSteps = list
    }

    /** Process start operator */
    static public final Operation executeOperation = Operation.EXECUTE
    /** Process error operator */
    static public final Operation errorOperation = Operation.ERROR

    /**
     * Define step in workflow
     * @param stepName operation step name
     * @param operation execute operation
     * @param cl defining code
     * @return step specification
     */
    protected WorkflowSpec step(String stepName, Operation operation,
                      @DelegatesTo(WorkflowSpec)
                      @ClosureParams(value = SimpleType, options = ['getl.models.opts.WorkflowSpec'])
                              Closure cl = null) {
        if (operation == errorOperation)
            throw new ExceptionModel('The "error" operation is not allowed in the top level of the workflow!')

        if (stepName == null) {
            def pName = operation.toString().capitalize()
            def lastStep = usedSteps.findAll {
                it.stepName.matches("(?i)$pName[ ]\\d+")
            }.collect {
                it.stepName.substring(4).toInteger()
            }.max()

            stepName = "$pName ${(lastStep?:0) + 1}"
        }

        checkModel()

        def parent = objectByName(stepName)
        if (parent == null) {
            if (usedSteps.find { it.operation == operation } != null)
                throw new ExceptionModel('It is allowed to specify no more than one \"$operation\" step in the workflow!')

            parent = newSpec(stepName, operation)
        }

        parent.runClosure(cl)

        return parent
    }

    /**
     * Define step in workflow
     * @param stepName operation step name
     * @param operation execute operation
     * @param cl defining code
     * @return step specification
     */
    WorkflowSpec step(String stepName,
                                @DelegatesTo(WorkflowSpec)
                                @ClosureParams(value = SimpleType, options = ['getl.models.opts.WorkflowSpec'])
                                        Closure cl = null) {
        def node = objectByName(stepName)
        if (node == null)
            throw new ExceptionModel("Step \"$stepName\" not found in workflow!")

        step(stepName, node.operation, cl)
    }

    /**
     * Define step in workflow
     * @param stepName operation step name
     * @param cl defining code
     * @return step specification
     */
    WorkflowSpec start(String stepName,
                       @DelegatesTo(WorkflowSpec)
                         @ClosureParams(value = SimpleType, options = ['getl.models.opts.WorkflowSpec'])
                              Closure cl = null) {
        step(stepName, executeOperation, cl)
    }

    /**
     * Define step in workflow
     * @param cl defining code
     * @return step specification
     */
    WorkflowSpec start(@DelegatesTo(WorkflowSpec)
                         @ClosureParams(value = SimpleType, options = ['getl.models.opts.WorkflowSpec'])
                              Closure cl = null) {
        step(null, executeOperation, cl)
    }

    @Override
    void checkObject(BaseSpec obj) {
        super.checkObject(obj)

        def step = obj as WorkflowSpec
        if (step.stepName == null)
            throw new ExceptionModel('Required name for step!')
        if (step.operation == null)
            throw new ExceptionModel('Required operation for \"${step.stepName}\" step!')

        step.condition()
        step.scripts.each { script ->
            step.detectRunClass(script.name as String)
        }
    }

    /** Start workflow processes */
    Integer execute() {
        dslCreator.logFinest("+++ Execute workflow \"$dslNameObject\" model ...")
        def res = 0
        usedSteps.each { node ->
            res =+ stepExecute(node)
        }
        dslCreator.logInfo("+++ Execution $res steps from workflow \"$dslNameObject\" model completed successfully")

        return res
    }

    /** Run workflow step */
    private Integer stepExecute(WorkflowSpec node, String parentStep = null) {
        def res = 0
        def stepLabel = (parentStep != null)?"${parentStep}.${node.stepName}":node.stepName
        dslCreator.logFinest("Start \"$stepLabel\" step ...")
        if (node.condition != null) {
            if (!dslCreator.runDsl(node.condition())) {
                dslCreator.logWarn("Conditions for \"$stepLabel\" step do not require its execute!")
                return res
            }
        }

        try {
            new Executor().with { exec ->
                dslCreator = this.dslCreator
                useList node.scripts
                setCountProc node.countThreads ?: 1
                abortOnError = true
                dumpErrors = true
                debugElementOnError = true
                run { Map<String, Object> script ->
                    exec.counter.nextCount()
                    def runClass = node.detectRunClass(script.name as String)
                    def scriptParams = script.params as Map<String, Object>
                    dslCreator.callScript(runClass, scriptParams)
                }
                res = counter.count
            }

            node.nested.findAll { it.operation != errorOperation }.each { subNode ->
                res += stepExecute(subNode, stepLabel)
            }
        }
        catch (Exception e) {
            def errStep = node.nested.find { it.operation == errorOperation }
            try {
                if (errStep != null)
                    stepExecute(errStep, stepLabel)
            }
            finally {
                throw e
            }
        }

        return res
    }
}