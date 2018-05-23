package getl.postgresql

import getl.jdbc.JDBCConnection
import getl.jdbc.JDBCDriverProto
import getl.utils.Config
import getl.utils.FileUtils

class PostgreSQLDriverTest extends JDBCDriverProto {
	static final def configName = 'tests/postgresql/postgresql.conf'
	@Override
	protected JDBCConnection newCon() {
		if (!FileUtils.ExistsFile(configName)) return null
		Config.LoadConfig(fileName: configName)
		return new PostgreSQLConnection(config: 'postgresql')
	}
}
