package getl.lang.sub

import getl.exception.ExceptionGETL
import groovy.transform.CompileStatic

/**
 * Storing a list of logins and passwords
 * @author Alexsey Konstantinov
 */
@CompileStatic
class StorageLogins extends HashMap<String, String> {
    StorageLogins(LoginManager manager) {
        if (manager == null)
            throw new ExceptionGETL('Required login manager object!')

        this.manager = manager
    }

    private LoginManager manager

    @Override
    String put(String login, String password) {
        return super.put(login, manager.encryptPassword(password))
    }

    @Override
    void putAll(Map logins) {
        def m = [:] as Map<String, String>
        logins.each { login, password ->
            m.put(login.toString(), manager.encryptPassword(password.toString()))
        }

        super.putAll(m)
    }

    /**
     * Copy logins with encrypted passwords
     * @param logins
     */
    void copyFrom(Map logins) {
        super.putAll(logins)
    }

    @Override
    String putIfAbsent(String login, String password) {
        return super.putIfAbsent(login, manager.encryptPassword(password))
    }
}