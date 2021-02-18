package getl.mysql

import getl.jdbc.JDBCConnection
import getl.jdbc.JDBCDriverProto
import getl.utils.Config
import getl.utils.FileUtils
import groovy.transform.InheritConstructors

@InheritConstructors
class MySQLDriverTest extends JDBCDriverProto {
	static final def configName = 'tests/mysql/mysql.conf'
	@Override
	protected JDBCConnection newCon() {
		if (!FileUtils.ExistsFile(configName)) return null
		Config.LoadConfig(fileName: configName)
		def con = new MySQLConnection(config: 'mysql')
		needCatalog = con.connectDatabase
		return con
	}
}
