package getl.models.sub

import com.cloudera.impala.jdbc42.internal.com.cloudera.altus.shaded.org.bouncycastle.util.Times
import getl.exception.ExceptionModel
import getl.lang.Getl
import getl.lang.sub.ScriptEvents
import getl.models.Workflows
import getl.utils.BoolUtils
import getl.utils.ConvertUtils
import getl.utils.DateUtils
import getl.utils.NumericUtils
import getl.utils.StringUtils

import java.sql.Timestamp

/**
 * Base class for extending workflow with custom code
 * @author Alexsey Konstantinov
 */
class WorkflowUserCode extends Getl {
    private final Map<String, Map<String, Object>> scriptVars = new HashMap<String, Map<String, Object>>()
    private final Map<String, ScriptEvents> scriptEvents = new HashMap<String, ScriptEvents>()

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

    /** Workflow model attributes */
    Map<String, Object> getModelAttrs() { currentModel.modelAttrs }

    /** Events on script */
    ScriptEvents events(String scriptName, @DelegatesTo(ScriptEvents) Closure cl = null) {
        if (scriptName == null || scriptName.length() == 0)
            throw new ExceptionModel('The script name is required for "vars" function!')

        if (currentModel.scriptByName(scriptName) == null)
            throw new ExceptionModel("There is script \"$scriptName\" specified in the vars function, which is not defined " +
                    "for model \"${currentModel.dslNameObject}\"!")

        scriptName = scriptName.toUpperCase()
        def res = scriptEvents.get(scriptName)
        if (res == null) {
            res = new ScriptEvents()
            scriptEvents.put(scriptName, res)
        }

        if (cl != null)
            res.tap(cl)

        return res
    }

    /** List of script in model */
    List<String> getModelScripts() { currentModel.listScripts().keySet().toList() }

    static Timestamp addDate(String dateType, Integer nb, Date date) {
        new Timestamp(DateUtils.AddDate(dateType, nb, date).time)
    }

    static java.sql.Date clearTime(Date date) {
        new java.sql.Date(DateUtils.ClearTime(date).time)
    }

    static java.sql.Date currentDate() {
        new java.sql.Date(DateUtils.CurrentDate().time)
    }

    static Long diffDate(Date date1, Date date2, String dateType, Boolean ignoreDST = false) {
        DateUtils.DiffDate(date1, date2, dateType, ignoreDST)
    }

    static Timestamp firstDateOfMonth(Date date) {
        new Timestamp(DateUtils.FirstDateOfMonth(date).time)
    }

    static String formatDate(String format, Date date) {
        DateUtils.FormatDate(format, date)
    }

    static String formatDate(Date date) {
        DateUtils.FormatDate(date)
    }

    static String formatTime(Date date) {
        DateUtils.FormatTime(date)
    }

    static String formatDateTime(Date date) {
        DateUtils.FormatDateTime(date)
    }

    static Integer lastDayOfMonth(Date date) {
        DateUtils.LastDayOfMonth(date)
    }

    static Integer lastDayOfMonth(Integer year, Integer month) {
        DateUtils.LastDayOfMonth(year, month)
    }

    static Timestamp lastDateOfMonth(Date date) {
        new Timestamp(DateUtils.LastDateOfMonth(date).time)
    }

    static Date lastDateOfMonth(Integer year, Integer month) {
        new Timestamp(DateUtils.LastDateOfMonth(year, month).time)
    }

    static Timestamp now() {
        new Timestamp(DateUtils.Now().time)
    }

    static Integer partOfDate(String partName, Date date) {
        DateUtils.PartOfDate(partName, date)
    }

    static java.sql.Date truncDay(Date date) {
        new java.sql.Date(DateUtils.TruncDay(date).time)
    }

    static Timestamp parseDate(String format, Object value, Boolean ignoreError = true) {
        new Timestamp(DateUtils.ParseDate(format, value, ignoreError).time)
    }

    static Map periodCrossing(List<Map> intervals) {
        DateUtils.PeriodCrossing(intervals)
    }

    static Timestamp truncTime(String part, Date date) {
        new Timestamp(DateUtils.TruncTime(part, date).time)
    }

    static Boolean isEven(Long value) {
        NumericUtils.IsEven(value)
    }

    static Boolean isMultiple(Long value, Long divider) {
        NumericUtils.IsMultiple(value, divider)
    }

    static BigDecimal round(BigDecimal value, Integer prec) {
        NumericUtils.Round(value, prec)
    }

    static Boolean isInteger(String value) {
        NumericUtils.IsInteger(value)
    }

    static Boolean isNumeric(String value) {
        NumericUtils.IsNumeric(value)
    }

    static String asString(Object value) {
        ConvertUtils.Object2String(value)
    }

    static BigDecimal asBigDecimal(Object value) {
        ConvertUtils.Object2BigDecimal(value)
    }

    static BigInteger asBigInteger(Object value) {
        ConvertUtils.Object2BigInteger(value)
    }

    static Integer asInteger(def value) {
        ConvertUtils.Object2Int(value)
    }

    static Long asLong(def value) {
        ConvertUtils.Object2Long(value)
    }

    static Double asDouble(def value) {
        ConvertUtils.Object2Double(value)
    }

    static List asList(def value) {
        if (value == null)
            return null
        if (value instanceof List)
            return value as List

        return ConvertUtils.String2List(value.toString())
    }

    static Map<String, Object> asMap(def value) {
        if (value == null)
            return null
        if (value instanceof Map)
            return value as Map

        return ConvertUtils.String2Map(value.toString())
    }

    static String addLedZeroStr(def s, Integer len) {
        StringUtils.AddLedZeroStr(s, len)
    }

    static String replicateStr(String c, Integer len) {
        StringUtils.Replicate(c, len)
    }

    static String leftStr(String s, Integer len) {
        StringUtils.LeftStr(s, len)
    }

    static String cutStr(String s, Integer maxLength) {
        StringUtils.CutStr(s, maxLength)
    }

    static String rightStr(String s, Integer len) {
        StringUtils.RightStr(s, len)
    }

    static String escapeStr(String str) {
        StringUtils.EscapeJavaWithoutUTF(str)
    }

    static String unescapeStr(String str) {
        StringUtils.UnescapeJava(str)
    }

    static String randomStr() {
        StringUtils.RandomStr()
    }

    static String toSnakeCaseStr(String text) {
        StringUtils.ToSnakeCase(text)
    }

    static String toCamelCaseStr(String text, Boolean capitalized = false) {
        StringUtils.ToCamelCase(text, capitalized)
    }

    static String generatePasswordStr(Integer length) {
        StringUtils.GeneratePassword(length)
    }

    static String formatNumber(Number value) {
        StringUtils.WithGroupSeparator(value)
    }

    static String encryptStr(String text, String password) {
        StringUtils.Encrypt(text, password)
    }

    static String decryptStr(String text, String password) {
        StringUtils.Decrypt(text, password)
    }

    static String nullIsEmptyStr(String value) {
        StringUtils.NullIsEmpty(value)
    }

    static Boolean asBoolean(def value, Boolean defaultValue = false) {
        BoolUtils.IsValue(value, defaultValue)
    }
}