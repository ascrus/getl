package getl.oracle

import getl.data.Field
import getl.jdbc.JDBCConnection
import getl.jdbc.JDBCDriverProto
import getl.jdbc.TableDataset
import getl.proc.Flow
import getl.tfs.TDS
import getl.utils.Config
import getl.utils.FileUtils
import groovy.transform.InheritConstructors
import org.h2.value.ValueTimestampTimeZone
import org.junit.Test

@InheritConstructors
class OracleDriverTest extends JDBCDriverProto {
	static final def configName = 'tests/oracle/oracle.conf'
	@Override
	protected JDBCConnection newCon() {
		if (!FileUtils.ExistsFile(configName)) return null
		Config.LoadConfig(fileName: configName)
		Locale.setDefault(new Locale('en','EN'))
		return new OracleConnection(config: 'oracle')
	}

	void testTZ() {
		def ds = new OracleTable(connection: con)
		ds.tableName = 'test_tz'
		ds.field << new Field(name: 'id', type: Field.integerFieldType, isKey: true)
		ds.field << new Field(name: 'dt', type: Field.datetimeFieldType)
		ds.field << new Field(name: 'dtz', type: Field.timestamp_with_timezoneFieldType)
		ds.drop(ifExists: true)
		ds.create()
		try {
			new Flow().writeTo(dest: ds) { updater ->
				Map r = [:]
				r.id = 1
				def d = new Date()
				r.dt = d
				r.dtz = d
				updater(r)
			}

			ds.field.clear()

			(ds.connection as JDBCConnection).sqlConnection.eachRow('select * from ' + ds.fullTableName) { r ->
				def t = r.dtz
				println t
				def x = new java.sql.Timestamp((t as oracle.sql.TIMESTAMPTZ).timestampValue(con.sqlConnection.connection).getTime())
				println x
			}

			ds.eachRow { r ->
				println r
			}
		}
		finally {
			ds.drop()
		}
	}
}