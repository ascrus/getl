package getl.mssql

import getl.data.*
import getl.jdbc.*
import getl.utils.*

class MSSQLDriverTest extends JDBCDriverProto {
	static final def configName = 'tests/mssql/mssql.conf'

	@Override
	protected JDBCConnection newCon() {
		if (!FileUtils.ExistsFile(configName)) return null
		Config.LoadConfig(fileName: configName)
		return new MSSQLConnection(config: 'mssql')
	}
}
