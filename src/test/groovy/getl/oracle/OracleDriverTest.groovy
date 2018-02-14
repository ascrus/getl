package getl.oracle

import getl.jdbc.JDBCConnection
import getl.jdbc.JDBCDriverProto
import getl.utils.Config
import getl.utils.FileUtils

class OracleDriverTest extends JDBCDriverProto {
	static final def configName = 'tests/oracle/oracle.conf'
	@Override
	protected JDBCConnection newCon() {
		if (!FileUtils.ExistsFile(configName)) return null
		Config.LoadConfig(configName)
		return new OracleConnection(config: 'oracle')
	}
}
