package getl.data

import com.fasterxml.jackson.annotation.JsonIgnore
import getl.driver.WebServiceDriver
import getl.exception.ExceptionGETL
import getl.utils.CloneUtils
import groovy.transform.InheritConstructors

@InheritConstructors
class WebServiceDataset extends FileDataset {
    @Override
    protected void registerParameters() {
        super.registerParameters()

        methodParams.register('readFromWeb', ['webServiceName', 'webParams', 'webVars', 'webRequestMethod'])
    }

    @Override
    protected void initParams() {
        super.initParams()

        params.webParams = [:] as Map<String, Object>
        params.webVars = [:] as Map<String, Object>
    }

    /** Current web service connection */
    @JsonIgnore
    WebServiceConnection getCurrentWebServiceConnection() { connection as WebServiceConnection }

    /** Web service name */
    String getWebServiceName() { params.webServiceName as String }
    /** Web service name */
    void setWebServiceName(String value) { params.webServiceName = value }

    /** Connection url parameters */
    Map<String, Object> getWebParams() { params.webParams as Map<String, Object> }
    /** Connection url parameters */
    void setWebParams(Map<String, Object> value) {
        webParams.clear()
        if (value != null)
            webParams.putAll(CloneUtils.CloneMap(value))
    }
    /** Connection url parameters */
    Map<String, Object> webParams() {
        def res = [:] as Map<String, Object>
        if (connection != null)
            res.putAll(currentWebServiceConnection.webParams)
        res.putAll(webParams)

        return res
    }

    /** Variable parameter values */
    Map<String, Object> getWebVars() { params.webVars as Map<String, Object> }
    /** Variable parameter values */
    void setWebVars(Map<String, Object> value) {
        webVars.clear()
        if (value != null)
            webVars.putAll(CloneUtils.CloneMap(value))
    }
    /** Variable parameter values */
    Map<String, Object> webVars() {
        def res = [:] as Map<String, Object>
        if (connection != null)
            res.putAll(currentWebServiceConnection.webVars)
        res.putAll(webVars)

        return res
    }

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
    /** Request method (GET or POST) */
    String webRequestMethod() { webRequestMethod?:(currentWebServiceConnection?.webRequestMethod)?:'GET' }

    /**
     * Read data from web service and save to file
     * @param wp web service read parameters
     */
    void readFromWeb(Map<String, Object> wp = [:]) {
        methodParams.validation('readFromWeb', wp)

        validConnection()

        if (fullFileName() == null)
            throw new ExceptionGETL('File name required!')

        (currentWebServiceConnection.driver as WebServiceDriver).readFromWeb(this, wp)
    }
}