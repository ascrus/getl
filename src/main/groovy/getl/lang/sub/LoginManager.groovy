package getl.lang.sub

import getl.exception.ExceptionGETL
import groovy.transform.CompileStatic

/**
 * Connection logins management
 * @author Alexsey Konstantinov
 */
@CompileStatic
class LoginManager {
    LoginManager(UserLogins owner) {
        if (owner == null)
            throw new ExceptionGETL('It is required to specify the connection object in the parameter!')

        this.owner = owner
    }

    /** Owner connection */
    private UserLogins owner

    /** Pushed logins */
    private Stack<String> pushLogins = new Stack<String>()

    /** Use specified login */
    void useLogin(String user) {
        if (!owner.storedLogins.containsKey(user))
            throw new ExceptionGETL("User \"$user\" not found in login repository!")

        //def pwd = owner.storedLogins.get(user)

        def reconnect = (owner.login != user && owner.isConnected())
        if (reconnect) owner.disconnect()
        owner.login = user
        owner.password = null
        if (reconnect) owner.connect()
    }

    /** Switch to new login */
    void switchToNewLogin(String user) {
        if (owner.login != null)
            pushLogins.push(user)
        else
            pushLogins.push('')
        owner.useLogin(user)
    }

    /** Go back to the last login */
    void switchToPreviousLogin() {
        if (pushLogins.isEmpty())
            throw new ExceptionGETL('There are no saved logins to switch to!')

        def lastLogin = pushLogins.pop()
        if (lastLogin.length() > 0)
            owner.useLogin(lastLogin)
        else {
            owner.login = null
            owner.password = null
        }
    }

    /** Return password for current login */
    String currentPassword() {
        if (owner.login == null)
            return owner.password

        if (owner.password != null)
            return owner.password

        return owner.storedLogins.get(owner.login)
    }

    /** Return decrypted password for current login */
    String currentDecryptPassword() {
        def pwd = currentPassword()
        if (pwd == null)
            return null

        if (owner instanceof GetlRepository) {
            def getl = owner as GetlRepository
            if (getl.dslCreator != null) {
                pwd = decryptPassword(pwd)
            }
        }

        return pwd
    }

    /**
     * Encrypt password
     * @param password original password
     * @return encrypted password
     */
    String encryptPassword(String password) {
        if (password == null)
            return null

        if (owner instanceof GetlRepository) {
            def getl = owner as GetlRepository
            if (getl.dslCreator != null) {
                getl.dslCreator.repositoryStorageManager.with {
                    if (storagePassword != null && !isLoadMode)
                        password = encryptText(password)

                    return true
                }
            }
        }

        return password
    }

    /**
     * Decrypt password
     * @param password encrypted password
     * @return decrypted password
     */
    String decryptPassword(String password) {
        if (password == null)
            return null

        if (owner instanceof GetlRepository) {
            def getl = owner as GetlRepository
            if (getl.dslCreator != null) {
                getl.dslCreator.repositoryStorageManager.with {
                    if (storagePassword != null)
                        password = decryptText(password)

                    return true
                }
            }
        }

        return password
    }

    /**
     * Decrypt object passwords
     * @return decrypted passwords
     */
    Map<String, Object> decryptObject() {
        def res = [:] as Map<String, Object>
        res.password = owner.password
        res.logins = [:] as Map<String, String>
        (res.logins as Map<String, String>).putAll(owner.storedLogins)

        if (!(owner instanceof GetlRepository))
            return res

        def getl = owner as GetlRepository
        if (getl.dslCreator == null || getl.dslCreator.repositoryStorageManager.storagePassword == null)
            return res

        def rp = getl.dslCreator.repositoryStorageManager

        res.password = rp.decryptText(res.password as String)
        def sl = [:] as Map<String, String>
        (res.logins as Map<String, String>).each {l, p ->
            if (p == null)
                sl.put(l, p)
            else
                sl.put(l, rp.decryptText(p))
        }
        res.logins = sl

        return res
    }

    /**
     * Encrypt passwords for an object
     * @param passwords object passwords
     */
    void encryptObject(Map<String, Object> passwords = null) {
        if (passwords == null) {
            passwords = [:] as Map<String, Object>
            if (owner.password != null)
                passwords.password = owner.password
            if (!owner.storedLogins.isEmpty()) {
                def ls = [:] as Map<String, String>
                passwords.logins = ls
                owner.storedLogins.each {l, p ->
                    ls.put(l, p)
                }
            }
        }

        if (passwords.containsKey('password'))
            owner.password = passwords.password as String

        if (passwords.containsKey('logins'))
            owner.storedLogins = passwords.logins as Map<String, String>
    }
}