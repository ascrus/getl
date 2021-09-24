//file:noinspection unused
package getl.models

import com.fasterxml.jackson.annotation.JsonIgnore
import getl.exception.ExceptionModel
import getl.lang.Getl
import getl.lang.sub.GetlRepository
import getl.models.opts.WorkflowScriptSpec
import getl.models.opts.WorkflowSpec
import getl.models.opts.WorkflowSpec.Operation
import getl.models.sub.BaseModel
import getl.models.sub.BaseSpec
import getl.proc.Executor
import getl.utils.BoolUtils
import getl.utils.CloneUtils
import getl.utils.DateUtils
import getl.utils.GenerationUtils
import getl.utils.Path
import getl.utils.StringUtils
import groovy.transform.InheritConstructors
import groovy.transform.Synchronized
import groovy.transform.stc.ClosureParams
import groovy.transform.stc.SimpleType
import java.lang.reflect.Modifier

/**
 * Description of the steps in the workflow
 * @author Alexsey Konstantinov
 */
@InheritConstructors
class Workflows extends BaseModel<WorkflowSpec> {
    /** List of used steps */
    @JsonIgnore
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

    /** Script execution results */
    private final Map<String, Map> _result = [:] as Map<String, Map>

    @JsonIgnore
    @Synchronized('_result')
    Map<String, Map> getResults() { _result }

    /**
     * Get the result of executing the specified script
     * @param scriptName script name
     * @return script result
     */
    Map result(String scriptName) {
        return _result.get(scriptName)?:[:]
    }

    @Synchronized('_result')
    void cleanResults() {
        _result.clear()
    }

    /** Process start operator */
    static public final Operation executeOperation = Operation.EXECUTE
    /** Process error operator */
    static public final Operation errorOperation = Operation.ERROR

    /** Return list of used step numbers */
    static private List<Integer> usedStepNumbers(List<WorkflowSpec> nodes) {
        def res = [] as List<Integer>

        nodes.each {node ->
            if (node.stepName.matches("(?i)STEP[ ]\\d+"))
                res.add(node.stepName.substring(4).toInteger())

            res.addAll(usedStepNumbers(node.nested))
        }

        return res
    }

    /** Return last used step number */
    protected Integer lastStepNumber() {
        return usedStepNumbers(usedSteps).max()?:0
    }

    /** Return default step name */
    String defaultStepName() { "STEP ${lastStepNumber() + 1}" }

    /** Return list of used step numbers */
    static private List<Integer> usedScriptNumbers(List<WorkflowSpec> nodes) {
        def res = [] as List<Integer>

        nodes.each {node ->
            node.scripts.each { name, scriptParams ->
                if (name.matches("(?i)SCRIPT[ ]\\d+"))
                    res.add(name.substring(6).toInteger())
            }
            res.addAll(usedScriptNumbers(node.nested))
        }

        return res
    }

    /** Return last used script number */
    protected Integer lastScriptNumber() {
        return usedScriptNumbers(usedSteps).max()?:0
    }

    /** Return default script name */
    String defaultScriptName() { "SCRIPT ${lastScriptNumber() + 1}" }

    /**
     * Find a step with a specified name in workflow
     * @param stepName step name
     * @return found step
     */
    WorkflowSpec stepByName(String stepName) {
        findStepByName(usedSteps, stepName)
    }

    /**
     * Find a step with a specified name in list of nodes */
    @SuppressWarnings('UnnecessaryQualifiedReference')
    static private WorkflowSpec findStepByName(List<WorkflowSpec> nodes, String stepName) {
        WorkflowSpec res = null

        nodes.each { node ->
            if (node.stepName == stepName) {
                res = node
                directive = Closure.DONE
                return
            }

            res = findStepByName(node.nested, stepName)
            if (res != null) {
                directive = Closure.DONE
                return
            }
        }

        return res
    }

    /**
     * Find a step with a specified name in workflow
     * @param scriptName script name
     * @return found step
     */
    WorkflowScriptSpec scriptByName(String scriptName) {
        def res = findNodeByScriptName(usedSteps, scriptName)
        return (res != null)?new WorkflowScriptSpec(res, true, res.findScript(scriptName)):null
    }

    /**
     * Find a step with a specified name in list of nodes */
    @SuppressWarnings('UnnecessaryQualifiedReference')
    static private WorkflowSpec findNodeByScriptName(List<WorkflowSpec> nodes, String scriptName) {
        WorkflowSpec res = null

        nodes.each { node ->
            node.scripts.each { name, scriptParams ->
                if (name == scriptName) {
                    res = node
                    directive = Closure.DONE
                    return
                }
            }
            if (res != null) {
                directive = Closure.DONE
                return
            }

            res = findNodeByScriptName(node.nested, scriptName)
            if (res != null) {
                directive = Closure.DONE
                return
            }
        }

        return res
    }

    /**
     * Refer to step in workflow
     * @param stepName operation step name
     * @param cl processing code
     * @return step specification
     */
    WorkflowSpec step(String stepName,
                      @DelegatesTo(WorkflowSpec)
                      @ClosureParams(value = SimpleType, options = ['getl.models.opts.WorkflowSpec'])
                              Closure cl = null) {
        def parent = stepByName(stepName)
        if (parent == null)
            throw new ExceptionModel("Step \"$stepName\" not found in workflow!")

        parent.runClosure(cl)

        return parent
    }

    /**
     * Refer to script in workflow
     * @param scriptName operation step name
     * @param cl processing code
     * @return step specification
     */
    WorkflowScriptSpec script(String scriptName,
                        @DelegatesTo(WorkflowScriptSpec)
                        @ClosureParams(value = SimpleType, options = ['getl.models.opts.WorkflowScriptSpec'])
                                Closure cl = null) {
        def parent = scriptByName(scriptName)
        if (parent == null)
            throw new ExceptionModel("Script \"$scriptName\" not found in workflow!")

        parent.runClosure(cl)

        return parent
    }

    /**
     * Define start in workflow
     * @param stepName operation step name
     * @param cl defining code
     * @return step specification
     */
    WorkflowSpec start(String stepName,
                       @DelegatesTo(WorkflowSpec)
                       @ClosureParams(value = SimpleType, options = ['getl.models.opts.WorkflowSpec'])
                               Closure cl = null) {
        if (stepName == null)
            throw new ExceptionModel('Step name required!')

        if (!usedSteps.isEmpty())
            throw new ExceptionModel('It is allowed to specify no more than one "start" step in the workflow!')

        checkModel()

        if (stepByName(stepName) != null)
            throw new ExceptionModel("The step named \"$stepName\" is already defined in the workflow!")

        def parent = newSpec(stepName, executeOperation)
        parent.runClosure(cl)

        return parent
    }

    /**
     * Define start in workflow
     * @param cl defining code
     * @return step specification
     */
    WorkflowSpec start(@DelegatesTo(WorkflowSpec)
                       @ClosureParams(value = SimpleType, options = ['getl.models.opts.WorkflowSpec']) Closure cl) {
        start(defaultStepName(), cl)
    }

    @Override
    void checkObject(BaseSpec obj) {
        super.checkObject(obj)

        def step = obj as WorkflowSpec
        if (step.stepName == null)
            throw new ExceptionModel('Required name for step!')
        if (step.operation == null)
            throw new ExceptionModel('Required operation for \"${step.stepName}\" step!')
    }

    /**
     * Start workflow processes
     * @param addVars variable for execution steps over step variables
     * @param conditionClassLoader class loader for running conditions
     * @param scriptClassLoader class loader for running specified script (closure parameter is passed the name of the class)
     * @return number of steps successfully completed
     */
    Integer execute(Map addVars = [:], URLClassLoader conditionClassLoader = null,
                    @ClosureParams(value = SimpleType, options = ['java.lang.String'])
                            Closure<URLClassLoader> scriptClassLoader = null) {
        dslCreator.logFinest("+++ Execute workflow \"$dslNameObject\" model ...")
        def conditions = generateConditions(conditionClassLoader)
        cleanResults()

        def res = 0
        usedSteps.each { node ->
            res =+ stepExecute(node, conditions, addVars?:[:], scriptClassLoader)
        }
        dslCreator.logInfo("--- Execution $res steps from workflow \"$dslNameObject\" model completed successfully")

        return res
    }

    /** Condition name from class of conditions */
    static private String conditionName(String stepName) {
        return "condition_${stepName.replace(' ', '')}"
    }

    /** Generate class of conditions */
    private Getl generateConditions(URLClassLoader classLoader) {
        def conditions = [:] as Map<String, String>
        findConditions(usedSteps[0], conditions)
        if (conditions.isEmpty())
            return null

        def className = "Workflow_${StringUtils.RandomStr().replace('-', '')}"
        def sb = new StringBuilder()
        sb.append """class $className extends getl.lang.Getl {
    public getl.models.Workflows proc
    Map result(String scriptName) { proc.result(scriptName) }\n"""

        conditions.each { stepName, condition ->
            sb.append "Boolean ${conditionName(stepName)}() { $condition }\n"
        }

        sb.append """}

return $className"""

        def classGenerated = GenerationUtils.EvalGroovyScript(sb.toString(), null, false, classLoader,
                dslCreator) as Class<Getl>
        def obj = dslCreator.callScript(classGenerated, [proc: this]).result as Getl

        return obj
    }

    /** Build list of conditions */
    private void findConditions(WorkflowSpec node, Map<String, String> conditions) {
        if (node.condition != null)
            conditions.put(node.stepName, node.condition)

        node.nested.each {findConditions(it, conditions) }
    }

    /** Run workflow step */
    private Integer stepExecute(WorkflowSpec node, Getl conditions, Map addVars,
                                @ClosureParams(value = SimpleType, options = ['java.lang.String'])
                                        Closure<URLClassLoader> scriptClassLoader,
                                String parentStep = null) {
        def res = 0
        def stepLabel = (parentStep != null)?"${parentStep}.${node.stepName}":node.stepName
        dslCreator.logFinest("Start \"$stepLabel\" step ...")
        if (node.condition != null) {
            def conditionName = conditionName(node.stepName)
            def conditionResult = conditions."$conditionName"()
            if (!BoolUtils.IsValue(conditionResult)) {
                dslCreator.logWarn("Conditions for \"$stepLabel\" step do not require its execute!")
                return res
            }
        }

        try {
            if (!node.scripts.isEmpty()) {
                new Executor().tap { exec ->
                    dslCreator = this.dslCreator
                    useList node.scripts.keySet().toList()
                    setCountProc node.countThreads ?: 1
                    abortOnError = true
                    dumpErrors = true
                    debugElementOnError = true
                    run { String scriptName ->
                        exec.counter.nextCount()

                        def scriptParams = node.scripts.get(scriptName)
                        def className = scriptParams.className as String
                        dslCreator.logFinest("Execute script \"$scriptName\" by class $className with step \"${node.stepName}\" ...")

                        URLClassLoader classLoader = null
                        if (scriptClassLoader != null)
                            classLoader = scriptClassLoader.call(className)
                        def runClass = classForExecute(className, classLoader, node.stepName)
                        if (runClass == null)
                            throw new ExceptionModel("Can't access class ${className} of step ${node.stepName}!")

                        def classParams = ReadClassFields(runClass)
                        def scriptVars = scriptParams.vars as Map<String, Object>
                        def execVars = [:] as Map<String, Object>
                        classParams.each { field ->
                            def fieldName = field.name as String
                            if (addVars.containsKey(fieldName))
                                execVars.put(fieldName, addVars.get(fieldName))
                            else if (scriptVars.containsKey(fieldName))
                                execVars.put(fieldName, scriptVars.get(fieldName))
                            else {
                                def objVar = node.variable(fieldName)
                                if (objVar != null)
                                    execVars.put(fieldName, objVar)
                            }
                        }

                        def scriptResult = dslCreator.callScript(runClass, execVars)
                        if (scriptResult.result != null && scriptResult.result instanceof Map) {
                            synchronized (_result) {
                                _result.put(scriptName, scriptResult.result as Map)
                            }
                        }
                    }
                    res = counter.count
                }
            }

            node.nested.findAll { it.operation != errorOperation }.each { subNode ->
                res += stepExecute(subNode, conditions, addVars, scriptClassLoader, stepLabel)
            }
        }
        catch (Exception e) {
            def errStep = node.nested.find { it.operation == errorOperation }
            try {
                if (errStep != null)
                    stepExecute(errStep, conditions, addVars, scriptClassLoader, stepLabel)
            }
            finally {
                throw e
            }
        }

        return res
    }

    /**
     * Get class for execute
     * @param className class name
     * @param stepName step name
     * @return class for execute
     */
    private Class<Getl> classForExecute(String className, URLClassLoader classLoader, String stepName) {
        if (className == null)
            throw new ExceptionModel("Required class name in step \"$stepName\"!")

        Class<Getl> res
        try {
            if (classLoader != null)
                res = Class.forName(className, true, classLoader) as Class<Getl>
            else
                res = Class.forName(className) as Class<Getl>
        }
        catch (Throwable e) {
            dslCreator.logError("Can not using class \"$className in step \"$stepName\": ${e.message}")
            throw e
        }

        if (!Getl.isAssignableFrom(res))
            throw new ExceptionModel("Class \"$className\" is not compatible with Getl class in step \"$stepName\"!")

        return res as Class<Getl>
    }

    /**
     * Read list of public field from Getl class
     * @param scriptClass Getl script class
     * @return list of field with attribute name, type and defaultValue
     */
    @SuppressWarnings('UnnecessaryQualifiedReference')
    static List<Map<String, Object>> ReadClassFields(Class<Getl> scriptClass) {
        if (scriptClass == null)
            throw new ExceptionModel('Required script class!')

        def res = [] as List<Map<String, Object>>

        def script = scriptClass.getDeclaredConstructor().newInstance()
        def fields = script.getClass().declaredFields.toList()
        script.getClass().fields.each { f ->
            if (fields.find { it.name == f.name } == null)
                fields.add(f)
        }
        fields.each { field ->
            def name = field.name
            def prop = script.hasProperty(name as String)
            if (groovy.lang.Closure.isAssignableFrom(prop.type))
                return

            def mod = prop.modifiers
            if (!Modifier.isPublic(mod) || Modifier.isStatic(mod))
                return

            def val = script[name]

            def p = [:] as Map<String, Object>
            p.name = name
            p.type = prop.type.name
            if (val != null)
                p.defaultValue = val

            res.add(p)
        }

        def convertType = { v ->
            def ret
            switch (v.getClass()) {
                case String:
                    ret = v
                    break

                case java.sql.Date:
                    ret = DateUtils.FormatDate('yyyy-MM-dd', v as java.sql.Date)
                    break

                case java.sql.Time:
                    ret = DateUtils.FormatDate('HH:mm:ss', v as java.sql.Time)
                    break

                case java.util.Date:
                    ret = DateUtils.FormatDate('yyyy-MM-dd HH:mm:ss', v as java.util.Date)
                    break

                case Path:
                    ret = (v as Path).mask
                    break

                default:
                    if (v instanceof GetlRepository)
                        ret = (v as GetlRepository).dslNameObject
                    else
                        ret = v.toString()
            }

            return ret
        }

        res.each { p ->
            if (p.defaultValue != null)
                p.defaultValue = convertType.call(p.defaultValue)
        }

        return res
    }
}