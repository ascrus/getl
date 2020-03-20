package getl.firebird

import getl.jdbc.JDBCConnection
import getl.jdbc.JDBCDriverProto
import getl.lang.Getl
import getl.utils.Config
import getl.utils.FileUtils
import org.junit.BeforeClass

class FirebirdDriverTest extends JDBCDriverProto {
    static final def configName = 'tests/firebird/firebird.conf'

    @BeforeClass
    static void CleanGetl() {
        Getl.CleanGetl()
    }

    @Override
    protected JDBCConnection newCon() {
        if (!FileUtils.ExistsFile(configName)) return null
        Config.LoadConfig(fileName: configName)
        def con = new FirebirdConnection(config: 'firebird')
        con.autoCommit = true
        con.connectProperty.encoding='utf8'
        return con
    }

    @Override
    protected void dropTable() {
        con.connected = false
        con.connected = true
        super.dropTable()
    }

    @Override
    protected String getCurrentTimestampFuncName() { 'CURRENT_TIMESTAMP' }

    protected boolean getTestSequence() { false }
}