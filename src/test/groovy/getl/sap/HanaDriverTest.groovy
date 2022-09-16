package getl.sap

import getl.jdbc.JDBCConnection
import getl.jdbc.JDBCDriverProto
import getl.utils.Config
import getl.utils.FileUtils
import groovy.transform.InheritConstructors

@InheritConstructors
class HanaDriverTest extends JDBCDriverProto {
    static final def configName = 'tests/sap/hana.conf'
    @Override
    protected JDBCConnection newCon() {
        if (!FileUtils.ExistsFile(configName)) return null
        Config.LoadConfig(fileName: configName)
        return new HanaConnection(config: 'hana')
    }

    @Override
    protected String getCurrentTimestampFuncName() { 'CURRENT_TIMESTAMP' }
}