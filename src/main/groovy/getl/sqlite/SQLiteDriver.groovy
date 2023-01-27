//file:noinspection UnnecessaryQualifiedReference
package getl.sqlite

import getl.data.Field
import getl.driver.Driver
import getl.jdbc.JDBCDriver
import getl.utils.ConvertUtils
import getl.utils.StringUtils
import groovy.transform.InheritConstructors

import java.util.regex.Pattern

/**
 * SQLite driver class
 * @author Alexsey Konstantinov
 */
@InheritConstructors
class SQLiteDriver extends JDBCDriver {
    @Override
    protected void initParams() {
        super.initParams()

        commitDDL = false
        caseObjectName = "UPPER"
        caseRetrieveObject = "UPPER"
        caseQuotedName = true
        fieldPrefix = '['
        fieldEndPrefix = ']'
        tablePrefix = '['
        tableEndPrefix = ']'
        localTemporaryTablePrefix = 'TEMPORARY'

        createViewTypes = ['CREATE']
        sqlExpressions.convertTextToTimestamp = 'DATETIME(\'{value}\')'
        sqlExpressions.now = 'DATETIME()'

        ruleEscapedText.put('\'', '\'\'')
        ruleEscapedText.remove('\n')
        ruleEscapedText.remove('\\')
    }

    List<Driver.Support> supported() {
        super.supported() +
                [Driver.Support.BLOB, Driver.Support.CLOB, Driver.Support.INDEX, Driver.Support.INDEXFORTEMPTABLE, Driver.Support.LOCAL_TEMPORARY,
                 Driver.Support.DATE, Driver.Support.TIME] - [Driver.Support.SCHEMA]
    }

    @Override
    List<Driver.Operation> operations() {
        return super.operations() - [Driver.Operation.CREATE_SCHEMA, Driver.Operation.TRUNCATE, Driver.Operation.RETRIEVELOCALTEMPORARYFIELDS]
    }

    @Override
    String defaultConnectURL() {
        return 'jdbc:sqlite:{database}'
    }

    String getNowFunc() { sqlExpressionValue('DATETIME()') }

    @Override
    void prepareField(Field field) {
        super.prepareField(field)

        if (field.type == Field.integerFieldType && field.typeName?.matches('(?i)BIGINT')) {
            field.type = Field.bigintFieldType
            field.dbType = java.sql.Types.BIGINT
            return
        }

        if (field.typeName != null) {
            if (field.typeName.matches('(?i)DATE')) {
                field.type = Field.Type.DATE
                field.dbType = java.sql.Types.DATE
                return
            }

            if (field.typeName.matches('(?i)TIME')) {
                field.type = Field.Type.TIME
                field.dbType = java.sql.Types.TIME
                return
            }

            if (field.typeName.matches('(?i)TIMESTAMP')) {
                field.type = Field.Type.DATETIME
                field.dbType = java.sql.Types.TIMESTAMP
                return
            }

            if (field.typeName.matches('(?i)BOOLEAN')) {
                field.type = Field.Type.BOOLEAN
                field.dbType = java.sql.Types.BOOLEAN
                return
            }

            if (field.typeName.matches('(?i)BLOB[(]\\d+[)]') ||
                    field.typeName.matches('(?i)BLOB')) {
                field.type = Field.Type.BLOB
                field.dbType = java.sql.Types.BLOB
                return
            }

            if (field.typeName.matches('(?i)CLOB[(]\\d+[)]') ||
                    field.typeName.matches('(?i)CBLOB')) {
                field.type = Field.Type.TEXT
                field.dbType = java.sql.Types.CLOB
                return
            }

            if (field.typeName.matches('(?i)DECIMAL[(]\\d+[)]')) {
                field.type = Field.Type.BIGINT
                field.dbType = java.sql.Types.BIGINT
                return
            }

            def decimalMask = '(?i)DECIMAL[(]\\s*(\\d+)\\s*[,]\\s*(\\d+)\\s*[)]'
            if (field.typeName.matches(decimalMask)) {
                def p = Pattern.compile(decimalMask)
                def m = p.matcher(field.typeName)
                if (m.find()) {
                    field.type = Field.Type.NUMERIC
                    field.dbType = java.sql.Types.DECIMAL
                    field.length = ConvertUtils.Object2Int(m[0][1])
                    field.precision = ConvertUtils.Object2Int(m[0][2])
                }
                //return
            }
        }
    }

    @Override
    Boolean blobReadAsObject (Field field = null) { return false }

    @Override
    String blobMethodWrite (String methodName) {
        return """void $methodName (java.sql.Connection con, java.sql.PreparedStatement stat, Integer paramNum, byte[] value) {
	if (value == null) { 
		stat.setNull(paramNum, java.sql.Types.BLOB) 
	}
	else {
		try (def stream = new ByteArrayInputStream(value)) {
		  stat.setBinaryStream(paramNum, stream, value.length)
		}
	}
}"""
    }

    @Override
    Boolean textReadAsObject () { return false }

    @Override
    String textMethodWrite (String methodName) {
        return """void $methodName (java.sql.Connection con, java.sql.PreparedStatement stat, Integer paramNum, String value) {
	if (value == null) { 
		stat.setNull(paramNum, java.sql.Types.CLOB) 
	}
	else {
		stat.setString(paramNum, value)
	} 
}"""
    }

    @Override
    String prepareReadField(Field field) {
        if (field.type == Field.booleanFieldType)
            return '({field} == 1)'

        if (field.type == Field.dateFieldType)
            return '(new java.sql.Date({field} as Long))'

        if (field.type == Field.timeFieldType)
            return '(new java.sql.Time({field} as Long))'

        if (field.type == Field.datetimeFieldType)
            return '(new java.sql.Timestamp({field} as Long))'

        if (field.type == Field.numericFieldType)
            return '(BigDecimal.valueOf({field} as Double))'

        return null
    }

    private String sessionID = StringUtils.RandomStr()

    @Override
    protected String sessionID() { sessionID }
}