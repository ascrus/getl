package getl.driver

import getl.data.WebServiceDataset
import getl.exception.ExceptionGETL
import getl.utils.DateUtils
import getl.utils.FileUtils
import getl.utils.WebUtils
import groovy.transform.InheritConstructors

@InheritConstructors
class WebServiceDriver extends FileDriver {
    static private UrlDateFormatter = DateUtils.BuildDateTimeFormatter('yyyy-MM-dd\'T\'HH:mm:ss.n')

    /**
     * Read data from web service and save to file
     * @wp webParams web service read parameters
     */
    void readFromWeb(WebServiceDataset dataset, Map < String, Object > wp) {
        def con = dataset.currentWebServiceConnection
        con.validPath()

        def url = con.webUrl
        if (url == null)
            throw new ExceptionGETL('It is required to set the server address in "webUrl"!')

        if (wp == null)
            wp = [:] as Map<String, Object>

        def connectTimeout = con.webConnectTimeout
        def readTimeout = con.webReadTimeout
        def requestMethod = (wp.webRequestMethod as String)?:dataset.webRequestMethod()
        def urlParams = (wp.webParams as Map<String, Object>)?:dataset.webParams()
        def urlVars = (wp.webVars as Map<String, Object>)?:dataset.webVars()
        def serviceName = (wp.webServiceName as String)?:dataset.webServiceName

        def serv = WebUtils.CreateConnection(url: url, service: serviceName, connectTimeout: connectTimeout,
                readTimeout: readTimeout, requestMethod: requestMethod, params: urlParams, vars: urlVars)
        WebUtils.DataToFile(serv, fullFileNameDataset(dataset))
    }
}