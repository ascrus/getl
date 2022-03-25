//file:noinspection unused
package getl.models

import com.fasterxml.jackson.annotation.JsonIgnore
import getl.exception.ExceptionModel
import getl.lang.Getl
import getl.lang.sub.GetlRepository
import getl.lang.sub.RepositorySave
import getl.models.opts.WorkflowScriptSpec
import getl.models.opts.WorkflowSpec
import getl.models.opts.WorkflowSpec.Operation
import getl.models.sub.BaseModel
import getl.models.sub.BaseSpec
import getl.models.sub.WorkflowUserCode
import getl.proc.Executor
import getl.utils.BoolUtils
import getl.utils.CloneUtils
import getl.utils.ConvertUtils
import getl.utils.DateUtils
import getl.utils.GenerationUtils
import getl.utils.Path
import getl.utils.StringUtils
import groovy.transform.InheritConstructors
import groovy.transform.Synchronized
import groovy.transform.stc.ClosureParams
import groovy.transform.stc.SimpleType
import java.lang.reflect.Modifier
import java.util.concurrent.ConcurrentHashMap

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
            p.remove('id')
            p.remove('index')
            list.add(new WorkflowSpec(own, p))
        }
        usedSteps = list
    }

    /** Script execution results */
    private final Map<String, Map> _result = new HashMap<String, Map>()

    @JsonIgnore
    @Synchronized('_result')
    Map<String, Map> getResults() { _result }

    /**
     * Get the result of executing the specified script
     * @param scriptName script name
     * @return script result
     */
    Map result(String scriptName) {
        if (scriptName == null || scriptName.length() == 0)
            throw new ExceptionModel("Required script name for result function!")

        return _result.get(scriptName.toUpperCase())?:new HashMap()
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
        stepName = stepName.toUpperCase()
        nodes.each { node ->
            if (node.stepName.toUpperCase() == stepName) {
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
        if (scriptName == null || scriptName.length() == 0)
            throw new ExceptionModel("Required script name for result function!")

        def res = findNodeByScriptName(usedSteps, scriptName)
        return (res != null)?new WorkflowScriptSpec(res, true, res.findScript(scriptName)):null
    }

    /**
     * Find a step with a specified name in list of nodes */
    @SuppressWarnings('UnnecessaryQualifiedReference')
    static private WorkflowSpec findNodeByScriptName(List<WorkflowSpec> nodes, String scriptName) {
        WorkflowSpec res = null
        scriptName = scriptName.toUpperCase()
        nodes.each { node ->
            node.scripts.each { name, scriptParams ->
                if (name.toUpperCase() == scriptName) {
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
        if (scriptName == null || scriptName.length() == 0)
            throw new ExceptionModel("Required script name for result function!")

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
        if (stepName == null || stepName.length() == 0)
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
     * @param userClassLoader class loader for running user script
     * @param scriptClassLoader class loader for running specified script (closure parameter is passed the name of the class)
     * @return number of steps successfully completed
     */
    Integer execute(Map<String, Object> addVars = null, List<String> include_steps = null, List<String> exclude_steps = null,
                    URLClassLoader userClassLoader = null,
                    @ClosureParams(value = SimpleType, options = ['java.lang.String'])
                            Closure<URLClassLoader> scriptClassLoader = null) {
        dslCreator.logFinest("Execute workflow [\"$dslNameObject\"] model ...")

        addVars = addVars?:new HashMap<String, Object>()
        generateUserCode(userClassLoader, addVars)
        cleanResults()

        def including = Path.Masks2Paths(include_steps, (modelVars + addVars) as Map<String, Map>)
        def excluding = Path.Masks2Paths(exclude_steps, (modelVars + addVars) as Map<String, Map>)

        def res = 0
        usedSteps.each { node ->
            try {
                res = +stepExecute(node, addVars, including, excluding, scriptClassLoader)
            }
            catch (Throwable e) {
                dslCreator.logError("Error execution step \"${node.stepName}\"!")
                throw e
            }
        }
        dslCreator.logInfo("--- Execution $res steps from workflow \"$dslNameObject\" model completed successfully")

        return res
    }

    /** Prepare step or script name */
    static private stepName2CodeName(String stepName) {
        return stepName.replace(' ', '_sss_').replace('.', '_qqq_')
                .replace('-', '_ddd_').replace(':', '_ggg_').toLowerCase()
    }

    /** Condition name from class of user codes */
    static private String conditionName(String stepName) {
        return "condition_${stepName2CodeName(stepName)}"
    }

    /** Initialization name from class of user codes */
    static private String initCodeName(String stepName) {
        return "init_${stepName2CodeName(stepName)}"
    }

    /** Finalization name from class of user codes */
    static private String finalCodeName(String stepName) {
        return "final_${stepName2CodeName(stepName)}"
    }

    /** Class script with step processing code */
    private String scriptUserCode
    /** Generated class with step processing code */
    private WorkflowUserCode generatedUserCode

    /** Generate class of user codes */
    private void generateUserCode(URLClassLoader classLoader, Map addVars) {
        def conditions = new HashMap<String, String>()
        findConditions(usedSteps[0], conditions)

        def inits = new HashMap<String, String>()
        findInitCode(usedSteps[0], inits)

        def finals = new HashMap<String, String>()
        findFinalCode(usedSteps[0], finals)

        if (conditions.isEmpty() && inits.isEmpty() && finals.isEmpty()) {
            scriptUserCode = null
            generatedUserCode = null
            return
        }

        def className = "Workflow_${StringUtils.RandomStr().replace('-', '')}"
        def sb = new StringBuilder()
        sb.append """package getl.user.workflow
import getl.data.*
import getl.utils.*
import getl.models.*
import getl.models.opts.*
import static getl.utils.DateUtils.AddDate as addDate
import static getl.utils.DateUtils.ClearTime as clearTime
import static getl.utils.DateUtils.CurrentDate as currentDate
import static getl.utils.DateUtils.DiffDate as diffDate
import static getl.utils.DateUtils.FirstDateOfMonth as firstDateOfMonth
import static getl.utils.DateUtils.FormatDate as formatDate
import static getl.utils.DateUtils.FormatTime as formatTime
import static getl.utils.DateUtils.FormatDateTime as formatDateTime
import static getl.utils.DateUtils.LastDateOfMonth as lastDateOfMonth
import static getl.utils.DateUtils.LastDayOfMonth as lastDayOfMonth
import static getl.utils.DateUtils.Now as now
import static getl.utils.DateUtils.PartOfDate as partOfDate
import static getl.utils.DateUtils.TruncDay as truncDay
import static getl.utils.NumericUtils.IsEven as isEven
import static getl.utils.NumericUtils.IsMultiple as isMultiple
import static getl.utils.NumericUtils.Round as round
import static getl.utils.StringUtils.AddLedZeroStr as addLedZero
import static getl.utils.StringUtils.LeftStr as leftStr
import static getl.utils.StringUtils.RightStr as rightStr
class $className extends getl.models.sub.WorkflowUserCode {
"""

        conditions.each { stepName, code ->
            sb.append "Boolean ${conditionName(stepName)}() {\n$code\n}\n"
        }

        inits.each { stepName, code ->
            sb.append "void ${initCodeName(stepName)}() {\n$code\n}\n"
        }

        finals.each { stepName, code ->
            sb.append "void ${finalCodeName(stepName)}() {\n$code\n}\n"
        }

        sb.append """}

return $className"""

        // println sb.toString()
        scriptUserCode = sb.toString()
        def classGenerated = GenerationUtils.EvalGroovyScript(scriptUserCode, null, false, classLoader, dslCreator) as Class<Getl>
        generatedUserCode = dslCreator.callScript(classGenerated, [currentModel: this], addVars).result as WorkflowUserCode
    }

    /** Build list of conditions code */
    private void findConditions(WorkflowSpec node, Map<String, String> list) {
        if (node.condition != null)
            list.put(node.stepName.toUpperCase(), node.condition)

        node.nested.each {findConditions(it, list) }
    }

    /** Build list of initialization code */
    private void findInitCode(WorkflowSpec node, Map<String, String> list) {
        if (node.initCode != null)
            list.put(node.stepName.toUpperCase(), node.initCode)

        node.nested.each {findInitCode(it, list) }
    }

    /** Build list of finalization code */
    private void findFinalCode(WorkflowSpec node, Map<String, String> list) {
        if (node.finalCode != null)
            list.put(node.stepName.toUpperCase(), node.finalCode)

        node.nested.each {findFinalCode(it, list) }
    }

    /** Run workflow step */
    private Integer stepExecute(WorkflowSpec node, Map addVars, List<Path> include_steps, List<Path> exclude_steps,
                                @ClosureParams(value = SimpleType, options = ['java.lang.String'])
                                        Closure<URLClassLoader> scriptClassLoader,
                                String parentStep = null) {
        if ((include_steps != null && !Path.MatchList(node.stepName, include_steps)) ||
                (exclude_steps != null && Path.MatchList(node.stepName, exclude_steps))) {
            dslCreator.logWarn("Step \"${node.stepName}\" is not included in the allowed step names and is skipped!")
            return 0
        }

        def res = 0
        def stepLabel = (parentStep != null)?"${parentStep}.${node.stepName}":node.stepName
        dslCreator.logFinest("Start \"$stepLabel\" step ...")

        def runMethod = { String methodType, String methodName, Boolean isConditional ->
            def resMethod = null
            try {
                if (isConditional)
                    resMethod = generatedUserCode."$methodName"()
                else
                    generatedUserCode."$methodName"()
            }
            catch (Throwable e) {
                dslCreator.logError("$methodType code execution error for step \"$stepLabel\"", e)
                dslCreator.logging.dump(e, 'workflow', "${dslNameObject}.$stepLabel", scriptUserCode)
                throw e
            }

            return resMethod
        }

        // Check condition for step
        if (node.condition != null) {
            def conditionResult = runMethod('Condition', conditionName(node.stepName), true)
            if (!BoolUtils.IsValue(conditionResult)) {
                dslCreator.logWarn("Conditions for \"$stepLabel\" step do not require its execute!")
                return res
            }
        }

        // Calc initialization code for step
        if (node.initCode != null) {
            runMethod('Initialization', initCodeName(node.stepName), false)
        }

        try {
            def classes =  new ConcurrentHashMap<String, Class<Getl>>()
            def isRepositorySave = false
            node.scripts.each { scriptName, scriptParams ->
                def className = scriptParams.className as String

                URLClassLoader classLoader = null
                if (scriptClassLoader != null)
                    classLoader = scriptClassLoader.call(className)
                def runClass = classForExecute(className, classLoader, node.stepName)
                if (runClass == null)
                    throw new ExceptionModel("Can't access class ${className} of step ${node.stepName}!")

                classes.put(scriptName.toUpperCase(), runClass)
                if (RepositorySave.isAssignableFrom(runClass))
                    isRepositorySave = true
            }
            if (isRepositorySave) {
                if (node.countThreads > 1)
                    throw new ExceptionModel("The number of threads in step \"${node.stepName}\" must be equal to 1 " +
                            "when using the script for saving repository objects!")
                classes.each { scriptName, runClass ->
                    if (!RepositorySave.isAssignableFrom(runClass))
                        throw new ExceptionModel("Script \"$scriptName\" cannot participate in step \"${node.stepName}\", " +
                                "because the script for saving repository objects is used!")
                }
            }

            if (!node.scripts.isEmpty()) {
                new Executor().tap { exec ->
                    dslCreator = this.dslCreator
                    useList node.scripts.keySet().toList()
                    setCountProc node.countThreads?:1
                    abortOnError = true
                    dumpErrors = false
                    debugElementOnError = false
                    def runScript = { String scriptName ->
                        exec.counter.nextCount()

                        def scriptParams = node.scripts.get(scriptName)
                        def className = scriptParams.className as String
                        dslCreator.logFinest("Execute script \"$scriptName\" by class $className with step \"${node.stepName}\" ...")

                        def runClass = classes.get(scriptName.toUpperCase())
                        def classParams = ReadClassFields(runClass)
                        def scriptVars = (scriptParams.vars as Map<String, Object>)?:(new HashMap<String, Object>())

                        if (generatedUserCode != null) {
                            def userVars = generatedUserCode.vars(scriptName)
                            if (userVars != null)
                                scriptVars.putAll(userVars)
                        }

                        scriptVars.each { name, val ->
                            if (val instanceof String || val instanceof GString)
                                scriptVars.put(name, StringUtils.EvalMacroString(val.toString(), modelVars + scriptVars + addVars, false))
                        }

                        def execVars = new HashMap<String, Object>()
                        classParams.each { field ->
                            def fieldName = field.name as String
                            def fieldType = field.type as String
                            if (!(fieldType in [Map.name, HashMap.name, List.name, ArrayList.name])) {
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
                            else if (fieldType in [Map.name, HashMap.name]) {
                                Map mapValue = null
                                Closure<Map> convertMap = { Map map, val ->
                                    if (val == null)
                                        return map

                                    if (map == null)
                                        map = new HashMap()
                                    if (val instanceof Map)
                                        map.putAll(val as Map)
                                    else if (val instanceof String)
                                        map.putAll(ConvertUtils.String2Map(val as String) as Map)
                                    else
                                        throw new ExceptionModel("It is not possible to convert " +
                                                "type \"${val.getClass().name}\" to type \"${Map.name}\"!")

                                    return map
                                }

                                mapValue = convertMap.call(mapValue, node.variable(fieldName))
                                mapValue = convertMap.call(mapValue, scriptVars.get(fieldName))
                                mapValue = convertMap.call(mapValue, addVars.get(fieldName))
                                if (mapValue != null)
                                    execVars.put(fieldName, mapValue)
                            }
                            else {
                                List listValue = null
                                Closure<List> convertList = { List list, val ->
                                    if (val == null)
                                        return list

                                    if (list == null)
                                        list = []
                                    if (val instanceof List)
                                        list.addAll(val as List)
                                    else if (val instanceof String)
                                        list.addAll(ConvertUtils.String2List(val as String))
                                    else
                                        throw new ExceptionModel("It is not possible to convert " +
                                                "type \"${val.getClass().name}\" to type \"${List.name}\"!")

                                    return list
                                }

                                listValue = convertList.call(listValue, node.variable(fieldName))
                                listValue = convertList.call(listValue, scriptVars.get(fieldName))
                                listValue = convertList.call(listValue, addVars.get(fieldName))
                                if (listValue != null)
                                    execVars.put(fieldName, listValue)
                            }
                        }

                        Map<String, Object> scriptResult
                        try {
                            scriptResult = dslCreator.callScript(runClass, execVars, addVars)
                        }
                        catch (Throwable e) {
                            dslCreator.logError("Error execution class \"${runClass.name}\"", e)
                            throw e
                        }
                        if (scriptResult.result != null && scriptResult.result instanceof Map) {
                            synchronized (_result) {
                                _result.put(scriptName.toUpperCase(), scriptResult.result as Map)
                            }
                        }

                        return true
                    }

                    if (!isRepositorySave)
                        run(runScript)
                    else {
                        node.scripts.each { scriptName, scriptParams ->
                            runScript.call(scriptName)
                        }
                    }

                    res = counter.count
                }
            }

            // Calc finalization code for step
            if (node.finalCode != null) {
                runMethod('Finalization', finalCodeName(node.stepName), false)
            }

            node.nested.findAll { it.operation != errorOperation }.each { subNode ->
                try {
                    res += stepExecute(subNode, addVars ?: new HashMap<String, Object>(), include_steps, exclude_steps, scriptClassLoader, stepLabel)
                }
                catch (Throwable e) {
                    dslCreator.logError("Error execution step \"${subNode.stepName}\"!")
                    throw e
                }
            }
        }
        catch (Exception e) {
            def errStep = node.nested.find { it.operation == errorOperation }
            if (errStep != null) {
                try {
                    stepExecute(errStep, addVars ?: new HashMap<String, Object>(), include_steps, exclude_steps, scriptClassLoader, stepLabel)
                }
                catch (Throwable err) {
                    dslCreator.logError("Error execution step \"${errStep.stepName}\"!", err)
                    throw err
                }
            }

            throw e
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
            dslCreator.logError("Can not using class \"$className in step \"$stepName\"", e)
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
    static List<Map<String, Object>> ReadClassFields(Class<Getl> scriptClass, Boolean ignoreClosure = false) {
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
            def mod = prop.modifiers
            if (!Modifier.isPublic(mod) || Modifier.isStatic(mod))
                return
            if (ignoreClosure && groovy.lang.Closure.isAssignableFrom(prop.type))
                return

            def val = script[name]

            def p = new HashMap<String, Object>()
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

    /** List of scripts */
    Map<String, WorkflowScriptSpec> listScripts() {
        return BuildListScripts(usedSteps)
    }

    /** Build list of scripts */
    static private Map<String, WorkflowScriptSpec> BuildListScripts(List<WorkflowSpec> nodes) {
        def res = new LinkedHashMap<String, WorkflowScriptSpec>()
        nodes.each { step ->
            step.scripts.each { name, script ->
                res.put(name, new WorkflowScriptSpec(step, true, script))
            }
            res.putAll(BuildListScripts(step.nested))
        }

        return res
    }

    @Override
    String toString() { super.toString() + ': ' + _result.toString() }
}