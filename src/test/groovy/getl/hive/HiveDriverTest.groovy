package getl.hive

import getl.data.*
import getl.jdbc.*
import getl.utils.*
import getl.lang.Getl

class HiveDriverTest extends JDBCDriverProto {
    static final def configName = 'tests/hive/hive.conf'

    @Override
    protected JDBCConnection newCon() {
        if (!FileUtils.ExistsFile(configName)) return null
        Config.LoadConfig(fileName: configName)
        return new HiveConnection(config: 'hive')
    }

    @Override
    List<Field> getFields () {
        def res = super.getFields()
        res.each { Field f -> if (f.type == Field.Type.STRING || f.type == Field.Type.TEXT) f.length = 255 }
        return res
    }

    @Override
    protected String getTableClass() { 'getl.hive.HiveTable' }

    @Override
    void createTable() {
        HiveTable t = table as HiveTable
        t.schemaName = con.connectDatabase
        t.drop(ifExists: true)
        t.field = fields
        t.create(storedAs: 'ORC', clustered: [by: ['id1'], intoBuckets: 2], tblproperties: [transactional: true])
    }

    @Override
    public TableDataset createPerfomanceTable(JDBCConnection con, String name, List<Field> fields) {
        HiveTable t = new HiveTable(connection: con, schema: con.connectDatabase, tableName: name, field: fields)
        t.drop(ifExists: true)
        t.create(storedAs: 'ORC', clustered: [by: ['id'], intoBuckets: 2], tblproperties: [transactional: true])
        return t
    }
}