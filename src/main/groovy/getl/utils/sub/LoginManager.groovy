package getl.utils.sub

import getl.exception.ExceptionGETL
import getl.lang.sub.UserLogins

/**
 * Connection logins management
 * @author Alexsey Konstantinov
 */
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

        def pwd = owner.storedLogins.get(user)

        def reconnect = (owner.login != user && owner.isConnected())
        if (reconnect) owner.disconnect()
        owner.login = user
        owner.password = pwd
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
}