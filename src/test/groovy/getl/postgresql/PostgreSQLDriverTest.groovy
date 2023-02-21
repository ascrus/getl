package getl.postgresql

import getl.jdbc.JDBCConnection
import getl.jdbc.JDBCDriverProto
import getl.lang.Getl
import getl.utils.Config
import getl.utils.DateUtils
import getl.utils.FileUtils
import getl.utils.StringUtils
import groovy.transform.InheritConstructors
import org.junit.BeforeClass
import org.junit.Test

@InheritConstructors
class PostgreSQLDriverTest extends JDBCDriverProto {
	static final def configName = 'tests/postgresql/postgresql.conf'

	@BeforeClass
	static void CleanGetl() {
		Getl.CleanGetl()
	}

	@Override
	protected JDBCConnection newCon() {
		if (!FileUtils.ExistsFile(configName)) return null
		Config.LoadConfig(fileName: configName)
		def con = new PostgreSQLConnection(config: 'postgresql')
		defaultSchema = 'public'
		needCatalog = con.connectDatabase
		return con
	}

	@Override
	protected String getCurrentTimestampFuncName() { 'CURRENT_TIMESTAMP' }

	@Override
	protected String getUseArrayType() { 'int4' }

	@Override
	void prepareTable() {
		table.fieldByName('id2').length = 6
		table.fieldByName('dtwithtz').length = 6
	}
}