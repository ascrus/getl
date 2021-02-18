package getl.mssql

import getl.data.*
import getl.jdbc.*
import getl.oracle.OracleTable
import getl.proc.Flow
import getl.utils.*
import groovy.transform.InheritConstructors
import org.junit.Test

import java.sql.Time
import java.sql.Timestamp
import java.time.Instant
import java.time.LocalDateTime
import java.time.ZoneId

@InheritConstructors
class MSSQLDriverTest extends JDBCDriverProto {
	static final def configName = 'tests/mssql/mssql.conf'

	@Override
	protected JDBCConnection newCon() {
		if (!FileUtils.ExistsFile(configName)) return null
		Config.LoadConfig(fileName: configName)
		def con = new MSSQLConnection(config: 'mssql')
		needCatalog = con.connectDatabase
		return con
	}

	void testTZ() {
		def ds = new MSSQLTable(connection: con)
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
				def t = r.dtz as microsoft.sql.DateTimeOffset
				println t
				def x = (t as microsoft.sql.DateTimeOffset).timestamp
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

	@Override
	protected String getCurrentTimestampFuncName() { 'CURRENT_TIMESTAMP' }

	protected boolean getTestSequence() { false }
}