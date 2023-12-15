package getl.db2

import getl.data.Field
import getl.driver.Driver
import getl.jdbc.JDBCDriver
import groovy.transform.InheritConstructors


/**
 * MSSQL driver class
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
class DB2Driver extends JDBCDriver {
	@Override
	protected void initParams() {
		super.initParams()

		//caseObjectName = 'UPPER'
		connectionParamBegin = ':'
		connectionParamJoin = ';'
		connectionParamFinish = ';'

		sqlExpressions.sequenceNext = 'SELECT NEXT VALUE FOR {value} AS id'
		sqlExpressions.changeSessionProperty = 'SET {name} = {value}'
		sqlExpressions.ddlChangeTypeColumnTable = 'ALTER TABLE {tableName} ALTER COLUMN {fieldName} SET DATA TYPE {typeName}'
	}

	@SuppressWarnings("UnnecessaryQualifiedReference")
	@Override
	List<Driver.Support> supported() {
		return super.supported() +
				[Driver.Support.GLOBAL_TEMPORARY, Driver.Support.SEQUENCE, Driver.Support.BLOB, Driver.Support.CLOB, Support.COLUMN_CHANGE_TYPE,
				 Driver.Support.INDEX, Support.INDEXFORTEMPTABLE, Driver.Support.TIME, Driver.Support.DATE]
	}

	/*@SuppressWarnings("UnnecessaryQualifiedReference")
	@Override
	List<Driver.Operation> operations() {
		return super.operations() +
                [Driver.Operation.TRUNCATE, Driver.Operation.DROP, Driver.Operation.EXECUTE,
				 Driver.Operation.CREATE]
	}*/

	@Override
    String defaultConnectURL () {
		return "jdbc:db2://{host}/{database}"
	}

	/** Current DB2 connection */
	@SuppressWarnings('unused')
	DB2Connection getCurrentDB2Connection() { connection as DB2Connection }
	
	@Override
    void prepareField (Field field) {
		super.prepareField(field)
		
		if (field.typeName?.matches('(?i)CLOB')) {
			field.type= Field.Type.STRING
		}
		else if (field.typeName?.matches('(?i)XML')) {
			field.type= Field.Type.STRING
		}
	}

	@Override
	String prepareReadField(Field field) {
		if (field.typeName?.matches('(?i)CLOB'))
			return '{field}.getSubString(1, (int){field}.length())'
		else if (field.typeName?.matches('(?i)XML'))
			return '{field}.getString()'

		return null
	}
}