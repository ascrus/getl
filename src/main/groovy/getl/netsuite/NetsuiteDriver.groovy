package getl.netsuite

import getl.driver.Driver
import getl.exception.ExceptionGETL
import getl.jdbc.JDBCDriver
import groovy.transform.InheritConstructors

/**
 * Netsuite driver class
 * @author Dmitry Shalind
 *
 */
@InheritConstructors
class NetsuiteDriver extends JDBCDriver {
	@Override
	protected void registerParameters() {
		super.registerParameters()

		methodParams.unregister('eachRow', ['onlyFields', 'excludeFields', 'where', 'order', 'offset',
											'queryParams', 'sqlParams', 'fetchSize', 'forUpdate', 'filter'])
	}

	@Override
	protected void initParams() {
		super.initParams()

		connectionParamBegin = ';'
		connectionParamJoin = ';'
		defaultSchemaName = 'Administrator'
		fieldPrefix = '['
		fieldEndPrefix = ']'
		tablePrefix = '['
		tableEndPrefix = ']'

		sqlExpressions.now = 'GETDATE()'
	}

	@SuppressWarnings("UnnecessaryQualifiedReference")
	@Override
	List<Driver.Support> supported() {
		return super.supported() + [Driver.Support.TIME, Driver.Support.DATE, Driver.Support.BOOLEAN] -
				[Driver.Support.VIEW]
	}

	/*@Override
	List<Operation> operations() {
		return [Operation.RETRIEVEFIELDS, Operation.READ_METADATA, Operation.INSERT, Operation.UPDATE, Operation.DELETE]
	}*/

	@SuppressWarnings("UnnecessaryQualifiedReference")
	@Override
	Map<String, Map<String, Object>> getSqlType () {
		def res = super.getSqlType()
		res.DOUBLE.name = 'float'
		res.BOOLEAN.name = 'bit'
		res.BLOB.name = 'varbinary'
		res.BLOB.useLength = JDBCDriver.sqlTypeUse.ALWAYS
		res.TEXT.name = 'varchar'
		res.TEXT.useLength = JDBCDriver.sqlTypeUse.ALWAYS
		res.DATETIME.name = 'datetime'

		return res
	}

	@SuppressWarnings('SpellCheckingInspection')
	@Override
	String defaultConnectURL () {
		return 'jdbc:ns://{host};ServerDataSource={serverDataSource};' +
                'encrypted=1;Ciphersuites={ciphersuites};' +
                'CustomProperties=(AccountID={accountId};RoleID=3)'
	}

	/**
	 * Build jdbc connection url
	 * @return
	 */
	@SuppressWarnings('SpellCheckingInspection')
	@Override
    protected String buildConnectURL () {
		def url = super.buildConnectURL()
		if (url == null) return null

		NetsuiteConnection con = connection as NetsuiteConnection

		if (url.indexOf('serverDataSource') != -1) {
			if (con.serverDataSource == null)
				throw new ExceptionGETL('Need set property "serverDataSource"')
			url = url.replace("{serverDataSource}", con.serverDataSource)
		}

		if (url.indexOf('ciphersuites') != -1) {
			if (con.ciphersuites == null)
				throw new ExceptionGETL('Need set property "ciphersuites"')
			url = url.replace("{ciphersuites}", con.ciphersuites)
		}

		if (url.indexOf('accountId') != -1) {
			if (con.accountId == null)
				throw new ExceptionGETL('Need set property "accountId"')
			url = url.replace("{accountId}", String.valueOf(con.accountId))
		}

		return url
	}
}