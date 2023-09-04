//file:noinspection unused
package getl.lang.sub

import getl.exception.RequiredParameterError
import groovy.transform.CompileStatic

/**
 * Storing a list of logins and passwords
 * @author Alexsey Konstantinov
 */
@CompileStatic
final class StorageLogins extends LinkedHashMap<String, String> {
    StorageLogins(LoginManager manager) {
        if (manager == null)
            throw new RequiredParameterError('manager', 'StorageLogins')

        this.manager = manager
    }

    private LoginManager manager

    @Override
    String put(String login, String password) {
        return super.put(login, manager.encryptPassword(password))
    }

    @Override
    void putAll(Map logins) {
        def m = new HashMap<String, String>()
        logins.each { login, password ->
            m.put(login.toString(), (password != null)?manager.encryptPassword(password.toString()):null as String)
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