package getl.netezza

import getl.jdbc.JDBCConnection
import getl.jdbc.JDBCDriverProto
import getl.utils.Config
import getl.utils.FileUtils
import groovy.transform.InheritConstructors

@InheritConstructors
class NetezzaDriverTest extends JDBCDriverProto {
    static final def configName = 'tests/netezza/netezza.conf'

    @Override
    protected JDBCConnection newCon() {
        if (!FileUtils.ExistsFile(configName)) return null
        Config.LoadConfig(fileName: configName)
        return new NetezzaConnection(config: 'netezza')
    }
}