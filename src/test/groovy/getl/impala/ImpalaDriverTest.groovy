package getl.impala

import getl.data.Dataset
import getl.data.Field
import getl.jdbc.JDBCConnection
import getl.jdbc.JDBCDriver
import getl.jdbc.JDBCDriverProto
import getl.utils.Config
import getl.utils.FileUtils
import getl.utils.Logs
import groovy.transform.InheritConstructors

@InheritConstructors
class ImpalaDriverTest extends JDBCDriverProto {
    static final def configName = 'tests/impala/impala.conf'

    @Override
    protected JDBCConnection newCon() {
        if (!FileUtils.ExistsFile(configName)) return null
        Config.LoadConfig(fileName: configName)
        return new ImpalaConnection(config: 'impala')
    }

    @Override
    String getTableClass() { 'getl.impala.ImpalaTable' }

    @Override
    protected void createTable() {
        ImpalaTable t = table as ImpalaTable
        t.schemaName = con.connectDatabase
        t.drop(ifExists: true)
        t.field = fields
        t.create(storedAs: 'PARQUET', sortBy: ['id1'], tblproperties: [transactional: false])
    }

    @Override
    String getDescriptionName() { "description" }
}