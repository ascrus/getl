package getl.data

import getl.driver.Driver
import getl.driver.WebServiceDriver
import getl.exception.ConnectionError
import getl.lang.Getl
import getl.lang.sub.LoginManager
import getl.lang.sub.StorageLogins
import getl.lang.sub.UserLogins
import getl.utils.CloneUtils
import getl.utils.ConvertUtils
import groovy.transform.InheritConstructors

@InheritConstructors
class WebServiceConnection extends FileConnection implements UserLogins {
    @Override
    protected Class<Driver> driverClass() { WebServiceDriver }

    @Override
    protected void registerParameters() {
        super.registerParameters()

        methodParams.register('Super', ['webUrl', 'webParams', 'webVars', 'webConnectTimeout',
                                        'webReadTimeout', 'webRequestMethod', 'autoCaptureFromWeb',
                                        'login', 'password', 'storedLogins', 'authType'])
    }

    @Override
    protected void initParams() {
        super.initParams()

        params.webParams = new HashMap<String, Object>()
        params.webVars = new HashMap<String, Object>()

        loginManager = new LoginManager(this)
        params.storedLogins = new StorageLogins(loginManager)
    }

    /** Url connection */
    String getWebUrl() { params.webUrl as String }
    /** Url connection */
    void setWebUrl(String value) { params.webUrl = value }

    /** Connection url parameters */
    Map<String, Object> getWebParams() { params.webParams as Map<String, Object> }
    /** Connection url parameters */
    void setWebParams(Map<String, Object> value) {
        webParams.clear()
        if (value != null)
            webParams.putAll(CloneUtils.CloneMap(value))
    }

    /** Variable parameter values */
    Map<String, Object> getWebVars() { params.webVars as Map<String, Object> }
    /** Variable parameter values */
    void setWebVars(Map<String, Object> value) {
        webVars.clear()
        if (value != null)
            webVars.putAll(CloneUtils.CloneMap(value))
    }

    /** Connection timeout in ms */
    Integer getWebConnectTimeout() { params.webConnectTimeout as Integer }
    /** Connection timeout in ms */
    void setWebConnectTimeout(Integer value) { params.webConnectTimeout = value }

    /** Read timeout in ms */
    Integer getWebReadTimeout() { params.webReadTimeout as Integer }
    /** Read timeout in ms */
    void setWebReadTimeout(Integer value) { params.webReadTimeout = value }

    /** Get request method */
    @SuppressWarnings('SpellCheckingInspection')
    static public final String WEBREQUESTMETHODGET = 'GET'
    /** Post request method */
    @SuppressWarnings('SpellCheckingInspection')
    static public final String WEBREQUESTMETHODPOST = 'POST'

    /** Request method (GET or POST) */
    String getWebRequestMethod() { params.webRequestMethod as String }
    /** Request method (GET or POST) */
    void setWebRequestMethod(String value) {
        if (value != null && !(value in [WEBREQUESTMETHODGET, WEBREQUESTMETHODPOST]))
            throw new ConnectionError(this, '#web_files.invalid_request', [request: value])

        params.webRequestMethod = value
    }

    /** Automatic data capture from a web service when reading rows */
    Boolean getAutoCaptureFromWeb() { ConvertUtils.Object2Boolean(params.autoCaptureFromWeb) }
    /** Automatic data capture from a web service when reading rows */
    void setAutoCaptureFromWeb(Boolean value) { params.autoCaptureFromWeb = value }

    /** Default timestamp with timezone mask */
    @SuppressWarnings('SpellCheckingInspection')
    static public String defaultTimestampWithTzFullMask = 'yyyy-MM-dd\'T\'HH:mm:ss[.SSSSSSSSS][.SSSSSS][.SSS]X'
    /** Default timestamp with timezone mask for format */
    @SuppressWarnings('SpellCheckingInspection')
    static public String defaultTimestampWithTzFullMaskFormat = 'yyyy-MM-dd\'T\'HH:mm:ss.SSSX'

    /** Format for timestamp with timezone fields */
    @Override
    String formatTimestampWithTz(Boolean formatValue = false) {
        return formatTimestampWithTz?:((formatValue)?defaultTimestampWithTzFullMaskFormat:defaultTimestampWithTzFullMask)
    }

    @Override
    Map<String, Object> attributes() { super.attributes() + webVars }

    /** Logins manager */
    private LoginManager loginManager

    /** Logins manager */
    LoginManager webLoginManager() { loginManager }

    @Override
    String getLogin() { params.login as String }
    @Override
    void setLogin(String value) { params.login = value }

    @Override
    String getPassword() { params.password as String }
    @Override
    void setPassword(String value) { params.password = loginManager.encryptPassword(value) }

    @Override
    Map<String, String> getStoredLogins() { params.storedLogins as Map<String, String> }
    @Override
    void setStoredLogins(Map<String, String> value) {
        storedLogins.clear()
        if (value != null) storedLogins.putAll(value)
    }

    @Override
    void useLogin(String user) {
        loginManager.useLogin(user)
    }

    @Override
    void switchToNewLogin(String user) {
        loginManager.switchToNewLogin(user)
    }

    @Override
    void switchToPreviousLogin() {
        loginManager.switchToPreviousLogin()
    }

    /** Authentication type */
    String getAuthType() { params.authType as String }
    /** Authentication type */
    void setAuthType(String value) {
        if (value != null) {
            value = value.toUpperCase()
            if (!(value in ['BASIC', 'NTLM']))
                throw new ConnectionError(this, '#utils.web.invalid_authentication_type', [type: value])
        }
        params.authType = value
    }

    @Override
    protected List<String> ignoreCloneClasses() { [StorageLogins.name] }

    @Override
    protected void afterClone(Connection original) {
        super.afterClone(original)

        def o = original as WebServiceConnection
        def passwords = o.loginManager.decryptObject()
        loginManager.encryptObject(passwords)
    }

    @Override
    void useDslCreator(Getl value) {
        def passwords = loginManager.decryptObject()
        super.useDslCreator(value)
        loginManager.encryptObject(passwords)
    }

    @Override
    protected void onLoadConfig(Map configSection) {
        super.onLoadConfig(configSection)
        loginManager.encryptObject()
    }
}