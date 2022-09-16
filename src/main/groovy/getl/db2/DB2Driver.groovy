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
	}

	@SuppressWarnings("UnnecessaryQualifiedReference")
	@Override
	List<Driver.Support> supported() {
		return super.supported() +
				[Driver.Support.GLOBAL_TEMPORARY, Driver.Support.SEQUENCE, Driver.Support.BLOB, Driver.Support.CLOB,
				 Driver.Support.INDEX, Support.INDEXFORTEMPTABLE, Driver.Support.TIME, Driver.Support.DATE, /*Driver.Support.TIMESTAMP_WITH_TIMEZONE,*/
				 Driver.Support.BOOLEAN]
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
	
	@Override
    void prepareField (Field field) {
		super.prepareField(field)
		
		if (field.typeName?.matches('(?i)CLOB')) {
			field.type= Field.Type.STRING
			field.getMethod = '{field}.getSubString(1, (int){field}.length())'
		}
		else if (field.typeName?.matches('(?i)XML')) {
			field.type= Field.Type.STRING
			field.getMethod = '{field}.getString()'
		} 
	}

	@Override
	protected String getChangeSessionPropertyQuery() { return 'SET {name} = {value}' }
}