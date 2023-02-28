//file:noinspection unused
//file:noinspection DuplicatedCode
package getl.utils

import getl.exception.ExceptionGETL
import getl.exception.IncorrectParameterError
import getl.exception.RequiredParameterError
import groovy.transform.CompileStatic
import groovy.transform.NamedVariant
import org.apache.hc.client5.http.auth.NTCredentials
import org.apache.hc.client5.http.classic.methods.HttpGet
import org.apache.hc.client5.http.config.RequestConfig
import org.apache.hc.client5.http.cookie.BasicCookieStore
import org.apache.hc.client5.http.impl.auth.BasicCredentialsProvider
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient
import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse
import org.apache.hc.client5.http.impl.classic.HttpClients
import org.apache.hc.core5.http.HttpHeaders
import org.apache.hc.core5.http.HttpStatus
import org.apache.hc.core5.http.message.BasicClassicHttpRequest
import org.apache.hc.core5.http.message.BasicHeader
import org.apache.hc.core5.util.Timeout
import org.apache.hc.client5.http.auth.AuthScope
import java.nio.charset.StandardCharsets
import java.sql.Time
import java.sql.Timestamp
import java.util.concurrent.TimeUnit

/**
 * Functions for working with web url connections
 * @author Alexsey Konstantinov
 */
@CompileStatic
class HttpClientUtils {
    /**
     * Build http get request
     * @param url source url
     * @param service service name
     * @param params requests parameters (use header.* for send parameters to headers)
     * @param vars evaluated variables for parameter value
     * @return http get request
     */
    @SuppressWarnings('GrDeprecatedAPIUsage')
    @NamedVariant
    static HttpGet BuildGetRequest(String url, String service = null, Map<String, Object> params = null, Map<String, Object> vars = null) {
        if (url == null)
            throw new RequiredParameterError('url', 'HttpClientUtils.BuildGetRequest')

        if (url[url.length() - 1] == '/')
            url = url.substring(0, url.length() - 1)

        if (service != null)
            service = URLEncoder.encode(service, StandardCharsets.UTF_8)

        String urlParams = null
        def headerParams = [:] as Map<String, String>
        if (params != null && !params.isEmpty()) {
            def up = [:] as Map<String, String>
            def isVars = (vars != null && !vars.isEmpty())
            params.each { key, value ->
                def isHeader = key.matches('(?i)^header[.].+')
                if (isHeader)
                    key = key.substring(7)

                if (value == null || (value.toString().length() == 0)) {
                    if (!isHeader)
                        up.put(key, '')

                    return
                }

                String val
                if (value instanceof String || value instanceof GString) {
                    if (isVars)
                        val = StringUtils.EvalMacroString((value as Object).toString(), vars, true) { v ->
                            Object res = null
                            if (v instanceof Timestamp)
                                res = WebUtils.UrlDateFormatter.format((v as Timestamp).toLocalDateTime()) + 'Z'
                            else if (v instanceof java.sql.Date)
                                res = WebUtils.UrlDateFormatter.format((v as java.sql.Date).toLocalDate()) + 'Z'
                            else if (v instanceof Time)
                                res = WebUtils.UrlDateFormatter.format((v as Time).toLocalTime()) + 'Z'
                            else if (v instanceof Date)
                                res = WebUtils.UrlDateFormatter.format((v as Date).toLocalDateTime()) + 'Z'

                            return res as Object
                        }
                    else
                        val = value.toString()
                }
                else if (value instanceof Timestamp)
                    val = WebUtils.UrlDateFormatter.format((value as Timestamp).toLocalDateTime()) + 'Z'
                else if (value instanceof java.sql.Date)
                    val = WebUtils.UrlDateFormatter.format((value as java.sql.Date).toLocalDate()) + 'Z'
                else if (value instanceof Time)
                    val = WebUtils.UrlDateFormatter.format((value as Time).toLocalTime()) + 'Z'
                else if (value instanceof Date)
                    val = WebUtils.UrlDateFormatter.format((value as Date).toLocalDateTime()) + 'Z'

                else
                    val = value.toString()

                if (!isHeader)
                    up.put(key, val)
                else
                    headerParams.put(key, val)
            }
            if (!up.isEmpty())
                urlParams = up.collect { key, value ->
                    return "${URLEncoder.encode(key, StandardCharsets.UTF_8)}=${URLEncoder.encode(value, StandardCharsets.UTF_8)}"
                }.join('&')
        }

        def request = new HttpGet(StringUtils.EvalMacroString('{url}{/%service%}{?%params%}', [url:url, service: service, params: urlParams]))
        headerParams.each { key, value ->
            request.addHeader(key, value)
        }

        request.setHeader(new BasicHeader('Pragma', 'no-cache'))
        request.setHeader(new BasicHeader('Cache-Control', 'no-cache'))
        request.setHeader(HttpHeaders.ACCEPT, '*/*')
        request.setHeader(HttpHeaders.ACCEPT_ENCODING, 'gzip, deflate, br')

        return request
    }

    /**
     *
     * @param authenticationType
     * @param login
     * @param password
     * @param connectTimeout
     * @param readTimeout
     * @return
     */
    static CloseableHttpClient BuildHttpClient(BasicClassicHttpRequest request, String authenticationType, String login = null, String password = null,
                                               Integer connectTimeout = null, Integer readTimeout = null) {
        def builder = HttpClients.custom()
        builder.setUserAgent('GetlFramework')

        def requestConfig = RequestConfig.custom()
        if (connectTimeout != null)
            requestConfig.setConnectTimeout(Timeout.of(connectTimeout, TimeUnit.SECONDS))
        if (readTimeout != null)
            requestConfig.setResponseTimeout(Timeout.of(readTimeout, TimeUnit.SECONDS))
        builder.setDefaultRequestConfig(requestConfig.build())

        def cookieStore = new BasicCookieStore()
        builder.setDefaultCookieStore(cookieStore)

        if (authenticationType != null) {
            if (login == null)
                throw new ExceptionGETL('#utils.web.non_login', [type: authenticationType])

            def uri = request.uri
            def authScope = new AuthScope(uri.host, uri.port)

            switch (authenticationType.toUpperCase()) {
                case 'BASIC':
                    /*def provider = new BasicCredentialsProvider()
                    provider.setCredentials(authScope, new UsernamePasswordCredentials(login, password.toCharArray()))
                    builder.setDefaultCredentialsProvider(provider)*/
                    def auth = login + ':' + password
                    def basicAuth = 'Basic ' + new String(Base64.getEncoder().encode(auth.bytes))
                    request.addHeader('Authorization', basicAuth)

                    break
                case 'NTLM':

                    def provider = new BasicCredentialsProvider()
                    def hostName = InetAddress.localHost.hostName
                    String domain = null
                    def i = login.indexOf('\\')
                    if (i > -1) {
                        domain = login.substring(0, i)
                        login = login.substring(i + 1)
                    }
                    provider.setCredentials(authScope, new NTCredentials(login, password.toCharArray(), hostName, domain))
                    builder.setDefaultCredentialsProvider(provider)
                    break
                default:
                    throw new IncorrectParameterError('authenticationType', '#utils.web.invalid_authentication_type', [type: authenticationType])
            }
        }

        return builder.build()
    }

    /**
     * Read web service data
     * @param connection web service connection
     * @param filePath path to save data to file
     * @param closeConnection close the connection when finished (default true)
     * @return web service response code
     */
    static Integer HttpResponseToFile(CloseableHttpResponse response, String filePath) {
        Integer res = null

        try {
            res = response.code
            if (res != HttpStatus.SC_OK)
                return res

            def tn = filePath + '.getltemp'
            def tFile = new File(tn)
            if (tFile.exists())
                tFile.delete()
            tFile.createNewFile()
            tFile.append(response.entity.content)

            def oFile = new File(filePath)
            if (oFile.exists())
                oFile.delete()
            if (!tFile.renameTo(oFile)) {
                tFile.delete()
                throw new ExceptionGETL('#io.file.fail_rename', [path: tFile.path, dir: oFile.path, detail: 'WebUtils.SaveResponseToFile'])
            }
        }
        finally {
            response.close()
        }

        return res
    }
}