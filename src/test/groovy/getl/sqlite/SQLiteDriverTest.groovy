package getl.sqlite

import getl.jdbc.JDBCConnection
import getl.jdbc.JDBCDriverProto
import getl.utils.Config
import getl.utils.FileUtils

class SQLiteDriverTest extends JDBCDriverProto {
    static final def configName = 'tests/sqlite/sqlite.conf'
    @Override
    protected JDBCConnection newCon() {
        if (!FileUtils.ExistsFile(configName)) return null
        Config.LoadConfig(fileName: configName)
        return new SQLiteConnection(config: 'sqlite')
    }

    @Override
    protected String getCurrentTimestampFuncName() { 'DATETIME()' }
}