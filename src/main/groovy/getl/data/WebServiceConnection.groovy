package getl.data

import getl.driver.Driver
import getl.driver.WebServiceDriver
import getl.exception.ExceptionGETL
import getl.utils.CloneUtils
import groovy.transform.InheritConstructors

@InheritConstructors
class WebServiceConnection extends FileConnection {
    @Override
    protected Class<Driver> driverClass() { WebServiceDriver }

    @Override
    protected void registerParameters() {
        super.registerParameters()

        methodParams.register('Super', ['webUrl', 'webParams', 'webVars', 'webConnectTimeout',
                                        'webReadTimeout', 'webRequestMethod'])
    }

    @Override
    protected void initParams() {
        super.initParams()

        params.webParams = [:] as Map<String, Object>
        params.webVars = [:] as Map<String, Object>
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
            throw new ExceptionGETL("Unknown request method \"$value\", allowed GET or POST!")

        params.webRequestMethod = value
    }
}