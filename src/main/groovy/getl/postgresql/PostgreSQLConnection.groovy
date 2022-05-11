//file:noinspection unused
package getl.postgresql

import com.fasterxml.jackson.annotation.JsonIgnore
import getl.driver.Driver
import getl.jdbc.JDBCConnection
import getl.jdbc.TableDataset
import groovy.transform.InheritConstructors

/**
 * PostgreSQL connection class
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
class PostgreSQLConnection extends JDBCConnection {
	@Override
	protected Class<Driver> driverClass() { PostgreSQLDriver }

	/** Current PostgreSQL connection driver */
	@JsonIgnore
	PostgreSQLDriver getCurrentPostgreSQLDriver() { driver as PostgreSQLDriver }

	@Override
	protected void registerParameters () {
		super.registerParameters()
		methodParams.register('Super', ['dateStyle'])
	}
	
	@Override
	protected Class<TableDataset> getTableClass() { PostgreSQLTable }

	/**
	 * Specifies the output format for date and time values, as well as the rules for interpreting ambiguous date values.
	 * For historical reasons, this variable contains two independent components: specifying the output format (ISO, Postgres, SQL, and German)
	 * and specifying the order year(Y)/month(M)/day(D) for input and output values (DMY, MDY, or YMD).<br><br>
	 * These two components can be specified separately or together. The keywords Euro and European are synonyms for DMY, and the keywords US,
	 * NonEuro, and NonEuropean are synonyms for MDY. The built-in default is ISO.
	 */
	String getDateStyle() { params.dateStyle as String }
	/**
	 * Specifies the output format for date and time values, as well as the rules for interpreting ambiguous date values.
	 * For historical reasons, this variable contains two independent components: specifying the output format (ISO, Postgres, SQL, and German)
	 * and specifying the order year(Y)/month(M)/day(D) for input and output values (DMY, MDY, or YMD).<br><br>
	 * These two components can be specified separately or together. The keywords Euro and European are synonyms for DMY, and the keywords US,
	 * NonEuro, and NonEuropean are synonyms for MDY. The built-in default is ISO.
	 */
	void setDateStyle(String value) {
		if (connected)
			changeDateStyle(value)

		params.dateStyle = value
	}
	/**
	 * Specifies the output format for date and time values, as well as the rules for interpreting ambiguous date values.
	 * For historical reasons, this variable contains two independent components: specifying the output format (ISO, Postgres, SQL, and German)
	 * and specifying the order year(Y)/month(M)/day(D) for input and output values (DMY, MDY, or YMD).<br><br>
	 * These two components can be specified separately or together. The keywords Euro and European are synonyms for DMY, and the keywords US,
	 * NonEuro, and NonEuropean are synonyms for MDY. The built-in default is ISO.
	 */
	String dateStyle() { dateStyle?:'ISO,YMD' }

	/**
	 * Change date style format for current session
	 * @param value
	 */
	void changeDateStyle(String value = null) {
		tryConnect()
		executeCommand('SET DATESTYLE = \'{style}\'', [queryParams: [style: value?:dateStyle()]])
	}

	@Override
	protected void doInitConnection () {
		super.doInitConnection()
		driverName = "org.postgresql.Driver"
	}

	@Override
	protected void doDoneConnect() {
		super.doDoneConnect()
		changeDateStyle()
	}
}