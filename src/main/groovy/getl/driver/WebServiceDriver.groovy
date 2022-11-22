package getl.driver

import getl.data.Dataset
import getl.data.WebServiceDataset
import getl.exception.ConnectionError
import getl.exception.DatasetError
import getl.exception.RequiredParameterError
import getl.lang.Getl
import getl.utils.HttpClientUtils
import getl.utils.Logs
import groovy.transform.InheritConstructors
import org.apache.http.HttpStatus

@InheritConstructors
class WebServiceDriver extends FileDriver {
    @Override
    protected void registerParameters() {
        super.registerParameters()
        methodParams.register('eachRow', ['autoCaptureFromWeb'])
    }

    /**
     * Read data from web service and save to file
     * @wp webParams web service read parameters
     */
    void readFromWeb(WebServiceDataset dataset, Map < String, Object > wp) {
        def con = dataset.currentWebServiceConnection
        con.validPath()

        def url = con.webUrl
        if (url == null)
            throw new RequiredParameterError(dataset, 'webUrl')

        if (wp == null)
            wp = new HashMap<String, Object>()

        def authType = con.authType
        def login = con.login
        def password = con.webLoginManager().currentDecryptPassword()

        if (authType != null && login == null)
            throw new DatasetError(dataset, '#utils.web.non_login', [type: authType])

        if (authType == null && login != null)
            throw new DatasetError(dataset, '#utils.web.invalid_login')

        if (password != null && login == null)
            throw new DatasetError(dataset, '#utils.web.invalid_password')

        def connectTimeout = con.webConnectTimeout
        def readTimeout = con.webReadTimeout
        def requestMethod = (wp.webRequestMethod as String)?:dataset.webRequestMethod()
        if (requestMethod.toUpperCase() != 'GET')
            throw new DatasetError(dataset, '#utils.web.allowed_only_get_method')
        def urlParams = (wp.webParams as Map<String, Object>)?:dataset.webParams()
        def urlVars = (wp.webVars as Map<String, Object>)?:dataset.webVars()
        def serviceName = (wp.webServiceName as String)?:dataset.webServiceName

        def attempts = con.numberConnectionAttempts?:1
        if (attempts < 1)
            throw new ConnectionError(con, '#params.great_zero', [param: 'numberConnectionAttempts', value: attempts])
        def timeout = con.timeoutConnectionAttempts?:1
        if (timeout < 1)
            throw new ConnectionError(con, '#params.great_zero', [param: 'timeoutConnectionAttempts', value: timeout])

        for (int retry = 0; retry <= con.numberConnectionAttempts; retry++) {
            def request = HttpClientUtils.BuildGetRequest(url, serviceName, urlParams, urlVars)
            Logs.Finest(con, '#web.connection.load_data', [url: request.uri.toString()])
            try(def client = HttpClientUtils.BuildHttpClient(request, authType, login, password, connectTimeout, readTimeout)
                def response = client.execute(request)) {
                def status = HttpClientUtils.HttpResponseToFile(response, fullFileNameDataset(dataset))
                if (status != HttpStatus.SC_OK)
                    throw new DatasetError(dataset, '#utils.web.invalid_status', [url: url, service: serviceName, status: status])

                break
            }
            catch (IOException e) {
                if (retry > con.numberConnectionAttempts)
                    throw e

                Getl.pause(timeout * 1000)
            }
        }
    }

    @Override
    Long eachRow(Dataset dataset, Map params, Closure prepareCode, Closure code) {
        super.eachRow(dataset, params, prepareCode, code)
        def ds = dataset as WebServiceDataset
        if (ds.autoCaptureFromWeb()) {
            if (ds.existsFile())
                ds.drop()
            ds.readFromWeb()
        }

        return 0L
    }
}