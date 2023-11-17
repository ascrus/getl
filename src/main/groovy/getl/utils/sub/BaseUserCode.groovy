//file:noinspection unused
package getl.utils.sub

import getl.data.Dataset
import getl.jdbc.QueryDataset
import getl.jdbc.TableDataset
import getl.jdbc.ViewDataset
import getl.lang.Getl
import getl.utils.*
import groovy.json.JsonParserType
import groovy.json.JsonSlurper
import groovy.transform.CompileStatic
import groovy.xml.XmlParser
import groovy.yaml.YamlSlurper
import java.sql.Time
import java.sql.Timestamp

/**
 * Base class for extending custom code
 * @author Alexsey Konstantinov
 */
@CompileStatic
class BaseUserCode extends Getl {
    static Timestamp addDate(String dateType, Integer nb, Date date) {
        DateUtils.AddDate(dateType, nb, date)
    }

    static java.sql.Date clearTime(Date date) {
        DateUtils.ClearTime(date)
    }

    static java.sql.Date currentDate() {
        DateUtils.CurrentDate()
    }

    static Long diffDate(Date date1, Date date2, String dateType, Boolean ignoreDST = false) {
        DateUtils.DiffDate(date1, date2, dateType, ignoreDST)
    }

    static Timestamp firstDateOfMonth(Date date) {
        DateUtils.FirstDateOfMonth(date)
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
        DateUtils.LastDateOfMonth(date)
    }

    static Date lastDateOfMonth(Integer year, Integer month) {
        DateUtils.LastDateOfMonth(year, month)
    }

    static Timestamp now() {
        DateUtils.Now()
    }

    static Integer partOfDate(String partName, Date date) {
        DateUtils.PartOfDate(partName, date)
    }

    static java.sql.Date truncDay(Date date) {
        DateUtils.TruncDay(date)
    }

    static Timestamp parseDate(String format, Object value, Boolean ignoreError = true) {
        new Timestamp(DateUtils.ParseDate(format, value, ignoreError).time)
    }

    static java.sql.Date parseSQLDate(Object value, Boolean ignoreError = true) {
        DateUtils.ParseSQLDate(value, ignoreError)
    }

    static Time parseSQLTime(Object value, Boolean ignoreError = true) {
        DateUtils.ParseSQLTime(value, ignoreError)
    }

    static Timestamp parseSQLTimestamp(Object value, Boolean ignoreError = true) {
        DateUtils.ParseSQLTimestamp(value, ignoreError)
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

    static Boolean isNumber(String value) {
        NumericUtils.IsNumber(value)
    }

    static Boolean isInteger(String value) {
        NumericUtils.IsInteger(value)
    }

    static Boolean isLong(String value) {
        NumericUtils.IsLong(value)
    }

    static Boolean isBigInteger(String value) {
        NumericUtils.IsBigInteger(value)
    }

    static Boolean isNumeric(String value) {
        NumericUtils.IsNumeric(value)
    }

    static Boolean isDouble(String value) {
        NumericUtils.IsDouble(value)
    }

    static Boolean isFloat(String value) {
        NumericUtils.IsFloat(value)
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

    static Float asFloat(def value) {
        ConvertUtils.Object2Float(value)
    }

    static Timestamp asTimestamp(def value) {
        ConvertUtils.Object2Timestamp(value)
    }

    static java.sql.Date asDate(def value) {
        ConvertUtils.Object2Date(value)
    }

    static Time asTime(def value) {
        ConvertUtils.Object2Time(value)
    }

    static List asList(def value) {
       ConvertUtils.Object2List(value)
    }

    static Map<String, Object> asMap(def value) {
        ConvertUtils.Object2Map(value)
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
        StringUtils.CutStrByWord(s, maxLength)
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

    static Boolean asBoolean(def value, Boolean defaultValue = null) {
        ConvertUtils.Object2Boolean(value, defaultValue)
    }

    /** Parse text to xml data */
    static Object parseXML(String text) {
        return (text != null)?new XmlParser().parseText(text):null
    }

    /** Parse text to json data */
    static Object parseJSON(String text) {
        def json = new JsonSlurper()
        return (text != null)?json.parseText(text):null
    }

    /** Parse text to json data with fast method */
    static Object parseFastJSON(String text) {
        def json = new JsonSlurper().tap { type = JsonParserType.INDEX_OVERLAY }
        return (text != null)?json.parseText(text):null
    }

    /** Parse text to json data with LAX method */
    static Object parseLaxJSON(String text) {
        def json = new JsonSlurper().tap { type = JsonParserType.LAX }
        return (text != null)?json.parseText(text):null
    }

    /** Parse text to yaml data */
    static Object parseYAML(String text) {
        return (text != null)?new YamlSlurper().parseText(text):null
    }

    /** Convert properties description text to Map structure */
    static Map parseProperties(String text, String fieldDelimited = '\n', String valueDelimited = '=') {
        return TransformUtils.DenormalizeColumn(text, fieldDelimited, valueDelimited)
    }

    /** Convert array description text to list structure */
    static List text2List(String text, String fieldDelimited = ';') {
        return TransformUtils.ListFromColumn(text, fieldDelimited)
    }

    /** Return default value if specified value is null */
    static Object isNull(def value, def defaultValue) {
        return value?:defaultValue
    }

    /** Return null if value equals specified value */
    static Object nullIf(def value, def nullValue) {
        return (value != nullValue)?value:null
    }

    /**
     * Compare value with conditions and return matching condition
     * @param args list of compared values and conditions and values: compared value, condition1, value1, condition2, value2, ...
     * @return matching value or null if nothing matched
     */
    static Object decode(Object ...args) {
       return ListUtils.Decode(args)
    }

    /** Check if dataset is table */
    static Boolean isTable(Dataset ds) {
        return (ds instanceof TableDataset)
    }

    /** Check if dataset is view */
    static Boolean isView(Dataset ds) {
        return (ds instanceof ViewDataset)
    }

    /** Check if dataset is query */
    static Boolean isQuery(Dataset ds) {
        return (ds instanceof QueryDataset)
    }

    /** Minimum value */
    static Number lesser(Number... values) {
        NumericUtils.Lesser(values)
    }

    /** Maximum value */
    static Number greatest(Number... values) {
        NumericUtils.Greatest(values)
    }

    /** Check path and create if not exists */
    static Boolean validPath(String path, Boolean deleteOnExit = false) {
        FileUtils.ValidPath(path, deleteOnExit)
    }

    /** Check path from file and create if not exists */
    static Boolean validFilePath(String path, Boolean deleteOnExit = false) {
        FileUtils.ValidFilePath(path, deleteOnExit)
    }

    /** Searches string for substring and returns an integer indicating the position of the character in string that is the first character of this occurrence */
    static Integer inStr(String text, String search, Integer position = 0, Integer occurrence = 1) {
        StringUtils.InStr(text, search, position, occurrence)
    }

    /** Returns a string that is a substring of this string */
    static String substr(String text, Integer start, Integer count = null) {
        StringUtils.Substr(text, start, count)
    }

    /** Returns a list of all occurrences of a regular expression */
    static List<String> findAll(String text, String regex) {
        StringUtils.FindAll(text, regex)
    }

    /** Converts all of the characters in this String to upper case */
    static String upper(String text) {
        text?.toUpperCase()
    }

    /** Converts all of the characters in this String to lower case */
    static String lower(String text) {
        text?.toLowerCase()
    }

    /** Returns a string whose value is this string, with all leading and trailing white space removed */
    static String trim(String text) {
        text?.strip()
    }

    /** Returns a string whose value is this string, with all leading white space removed */
    static String lTrim(String text) {
        text?.stripLeading()
    }

    /** Returns a string whose value is this string, with all trailing white space removed */
    static String rTrim(String text) {
        text?.stripTrailing()
    }
}