package getl.lang.sub

/**
 * Description of the interface of objects that use logins
 * @author Alexsey Konstantinov
 */
interface UserLogins {
    /** User login */
    String getLogin()
    /** User login */
    void setLogin(String value)

    /** User password */
    String getPassword()
    /** User password */
    void setPassword(String value)

    /** Login repository */
    Map<String, String> getStoredLogins()
    /** Login repository */
    void setStoredLogins(Map<String, String> value)

    /** Use specified login */
    void useLogin(String user)

    /** Switch to new login */
    void switchToNewLogin(String user)

    /** Switch to previous login */
    void switchToPreviousLogin()

    /** Connect to source */
    void connect()

    /** Disconnect from source */
    void disconnect()

    /** Established connection flag */
    Boolean isConnected()
}