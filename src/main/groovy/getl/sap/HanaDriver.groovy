package getl.sap

import getl.data.Field
import getl.driver.Driver
import getl.jdbc.JDBCDriver
import groovy.transform.InheritConstructors

/**
 * SAP Hana driver class
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
class HanaDriver extends JDBCDriver {
    @Override
    protected void initParams() {
        super.initParams()

        caseObjectName = 'UPPER'
        caseRetrieveObject = 'UPPER'

        sqlExpressions.sysDualTable = 'dummy'
        sqlExpressions.sequenceNext = 'SELECT {value}.NEXTVAL AS id FROM DUMMY'
        sqlExpressions.changeSessionProperty = 'SET TEMPORARY OPTION {name} = {value}'
    }

    @Override
    Map<String, Map<String, Object>> getSqlType () {
        def res = super.getSqlType()
        res.DOUBLE.name = 'double'
        res.UUID.name = 'uniqueidentifier'
        res.BLOB.name = 'VARBINARY'
        res.BLOB.defaultLength = 5000
        res.TEXT.useLength = sqlTypeUse.NEVER

        return res
    }

    @Override
    List<Support> supported() {
        return super.supported() +
                [Support.LOCAL_TEMPORARY, Support.GLOBAL_TEMPORARY, Support.SEQUENCE, Support.BLOB, Support.CLOB, Support.TIME, Support.DATE,
                 Support.COMPUTE_FIELD, Support.INDEX] - [Support.CHECK_FIELD, Support.SELECT_WITHOUT_FROM]
    }

    @SuppressWarnings("UnnecessaryQualifiedReference")
    @Override
    List<Driver.Operation> operations() {
        return super.operations() - [Driver.Operation.RETRIEVELOCALTEMPORARYFIELDS]
    }

    @Override
    String defaultConnectURL () {
        return 'jdbc:sap://{host}/{database}'
    }

    /** Current SAP Hana connection */
    @SuppressWarnings('unused')
    HanaConnection getCurrentHanaConnection() { connection as HanaConnection }

    @Override
    protected String sessionID() {
        String res = null
        //noinspection SqlNoDataSourceInspection
        def rows = sqlConnect.rows('SELECT CONNECTION_ID FROM m_active_statements')
        if (!rows.isEmpty())
            res = rows[0].connection_id.toString()

        return res
    }

    @Override
    String generateComputeDefinition(Field f) { "GENERATED ALWAYS AS (${f.compute})" }

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
    String blobMethodWrite(String methodName) {
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
    Boolean blobReadAsObject(Field field = null) { field.typeName == 'BLOB' }
}