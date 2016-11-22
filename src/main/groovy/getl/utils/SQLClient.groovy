/*
 GETL - based package in Groovy, which automates the work of loading and transforming data. His name is an acronym for "Groovy ETL".

 GETL is a set of libraries of pre-built classes and objects that can be used to solve problems unpacking,
 transform and load data into programs written in Groovy, or Java, as well as from any software that supports
 the work with Java classes.
 
 Copyright (C) 2013-2015  Alexsey Konstantonov (ASCRUS)

 This program is free software: you can redistribute it and/or modify
 it under the terms of the GNU Lesser General Public License as published by
 the Free Software Foundation, either version 3 of the License, or
 (at your option) any later version.

 This program is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU General Public License for more details.

 You should have received a copy of the GNU General Public License and
 GNU Lesser General Public License along with this program.
 If not, see <http://www.gnu.org/licenses/>.
*/

package getl.utils

import getl.proc.*
import getl.jdbc.*

class SQLClient extends Job {
	
	static main(args) {
		new SQLClient().run(args)
	}

	@Override
	public void process() {
		println "SQL client, (c) EasyData LTD"
		if (args."connection" == null && args."script" == null) {
			println """
*** Run syntax:"
SQLScripter config.filename=<Config file name> connection=<Connection name> script=<Script file name> var.<Variable name>=<Variable value>"

*** Config syntax:"
{
  "log": {
    "file": "<Log file name>"
  },

  "connections": {
    "<Connection name>": {
      "connection": "<Connection class>",
      "connectionURL": "<Connection url path>",
      "login": "<Connect login>",
      "password": "<Connect password>"
    }
  },

  "vars": {
    "<Variable name>": "<Variable value>"
  }
}

*** Avaible connection classes:
getl.h2.H2Connection
getl.mssql.MSSQLConnection
getl.mysql.MySQLConnection
getl.oracle.OracleConnection
getl.postgresql.PostgreSQLConnection
getl.vertica.VerticaConnection

*** Script command:
{<Variable name>}
- include variable value to script

ECHO
- echo text to console

SET SELECT <Select query> ;
- take the field values first row to the variables

IF <Condition>;
<SQL operations>
END IF;
- execute operations if condition true

FOR SELECT <Select query> ;
<SQL operations>
END FOR;
- execute operations for each rows

/*:<Variable name>*/
<DML operator>
- run DML operator and set processed row count to variable

Example script with variable var.RunVar=DEMO:
IF '{RunVar}' = 'DEMO';
  ECHO RunVar has value: {RunVar}
END IF;

ECHO Process table1 rows
FOR SELECT id AS group_id FROM table1 ORDER BY id;
  /*:update_count*/ 
  UPDATE table2 SET value + 1 WHERE group_id = {group_id};
  ECHO Update {update_count} rows for {group_id} group

  SET SELECT Avg(value) AS avg_value FROM table2 WHERE group_id = {group_id};
  ECHO Detected {avg_value} average value for {group_id} group
END FOR;"""
			return
		}
		
		String connectionName = args."connection"
		JDBCConnection connection = JDBCConnection.CreateConnection(config: connectionName) as JDBCConnection
		
		
		getl.jdbc.SQLScripter scripter = new getl.jdbc.SQLScripter(connection: connection, logEcho: "INFO")
		scripter.vars.putAll(Config.content."vars" as Map)
		scripter.loadFile(args."script" as String, "utf-8")
		scripter.runSql()
		Logs.Info("Finish, ${scripter.rowCount} rows processed")
	}
}
