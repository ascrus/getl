package getl.job.jdbc

import getl.lang.Getl
import getl.utils.FileUtils
import groovy.transform.BaseScript
import groovy.transform.Field

//noinspection GroovyUnusedAssignment
@BaseScript Getl main

@Field String connection = null
@Field String login = null
@Field String path = null
@Field String files = null
@Field Boolean allow_procedure_statements
@Field Map<String, Object> ext = null

void check() {
    assert connection != null, 'It is required to specify the name of the connection to the JDBC server in the "connection" parameter!'
    assert files != null, 'You need to specify a list of file names in the "files" parameter!'
}

def con = jdbcConnection(connection) {
    if (this.login != null)
        useLogin this.login
}

def scripts = files.split(',')

logFine "*** Executing SQL scripts on the JDBC server"
logFine "  server: $con"
if (login != null)
    logFine "  login: $login"
if (path != null)
    logFine "  path for files: $path"
logFine "  files: ${scripts.join(', ')}"

logFinest "Connect to server $con ..."
con.connected = true

def res = new HashMap<String, Object>()

//noinspection GroovyVariableNotAssigned
sql {scripter ->
    useConnection con
    con.transaction(true) {
        scripts.each { fileName ->
            fileName = FileUtils.AddExtension(fileName, 'sql')
            logFinest "Executing SQL script file \"$fileName\" ..."
            def fn = ((path != null)?(path + '/'):'') + fileName
            if (ext != null)
                vars = ext
            runFile allow_procedure_statements as Boolean, fn
            logInfo "SQL script file \"$fileName\" executed successfully"
            res.putAll(vars)
        }
    }
}

return res