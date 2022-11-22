//file:noinspection unused
package getl.utils

import getl.exception.ExceptionGETL
import getl.lang.Getl
import groovy.transform.CompileStatic
import groovy.transform.NamedVariant
import java.nio.charset.StandardCharsets
import java.time.format.DateTimeFormatter

/**
 * Functions for working with web url connections
 * @author Alexsey Konstantinov
 */
@CompileStatic
class WebUtils {
    /** Get request method */
    @SuppressWarnings('SpellCheckingInspection')
    static public final String WEBREQUESTMETHODGET = 'GET'
    /** Post request method */
    @SuppressWarnings('SpellCheckingInspection')
    static public final String WEBREQUESTMETHODPOST = 'POST'

    @SuppressWarnings('SpellCheckingInspection')
    static public final DateTimeFormatter UrlDateFormatter = DateUtils.BuildDateTimeFormatter('yyyy-MM-dd\'T\'HH:mm:ss.n')

    /**
     * Create a connection to a web service
     * @param url server connection URL
     * @param login login for basic authentication
     * @param login password for basic authentication (required if login using)
     * @param service web service name
     * @param connectTimeout connection timeout in ms
     * @param readTimeout read timeout in ms
     * @param requestMethod request method (GET or POST)
     * @param params connection parameters
     * @param vars variables for parameters
     * @return created connection to web service
     */
    @SuppressWarnings('GrDeprecatedAPIUsage')
    @NamedVariant
    static HttpURLConnection CreateConnection(String url, String login = null, String password = null, String service = null, Integer connectTimeout = null,
                                              Integer readTimeout = null, String requestMethod = null, Map<String, Object> params = null, Map<String, Object> vars = null) {
        if (requestMethod == null)
            throw new ExceptionGETL('Required "requestMethod" parameter value!')
        if (!(requestMethod in [WEBREQUESTMETHODGET, WEBREQUESTMETHODPOST]))
            throw new ExceptionGETL("Unknown request method \"$requestMethod\"!")

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
                            (v instanceof Date) ? (UrlDateFormatter.format((v as Date).toLocalDateTime()) + 'Z') : v.toString()
                        }
                    else
                        val = value.toString()
                }
                else if (value instanceof Date)
                    val = UrlDateFormatter.format((value as Date).toLocalDateTime()) + 'Z'
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

        def servUrl = StringUtils.EvalMacroString('{url}{/%service%}{?%params%}', [url: url, service: service, params: urlParams])
        def serv = new URL(servUrl).openConnection() as HttpURLConnection

        if (login != null) {
            if (password == null)
                throw new ExceptionGETL("Password not set in connection \"$url\" for login \"$login\"")

            def auth = login + ':' + password
            def basicAuth = 'Basic ' + new String(Base64.getEncoder().encode(auth.bytes))
            serv.setRequestProperty('Authorization', basicAuth)
        }

        if (connectTimeout != null)
            serv.connectTimeout = connectTimeout
        if (readTimeout != null)
            serv.readTimeout = readTimeout
        serv.requestMethod = requestMethod
        headerParams.each { key, value ->
            serv.setRequestProperty(key, value)
        }

        serv.setUseCaches(false)

        return serv
    }

    /**
     * Read web service data
     * @param connection web service connection
     * @param filePath path to save data to file
     * @param closeConnection close the connection when finished (default true)
     * @return web service response code
     */
    static Integer DataToFile(HttpURLConnection serv, String filePath, Boolean closeConnection = true) {
        Integer res = null
        closeConnection = BoolUtils.IsValue(closeConnection)

        try {
            res = serv.responseCode
            if (res != serv.HTTP_OK) {
                throw new ExceptionGETL('Error reading service "{url}", code: {code}{, response: %resp%}',
                        [url: serv.getURL(), code: res, resp: serv.responseMessage])
            }

            def tn = filePath + '.getltemp'
            def tFile = new File(tn)
            if (tFile.exists())
                tFile.delete()
            tFile.createNewFile()
            tFile.append(serv.inputStream)

            def oFile = new File(filePath)
            if (oFile.exists())
                oFile.delete()
            if (!tFile.renameTo(oFile)) {
                tFile.delete()
                throw new ExceptionGETL("Can not rename file \"$tn\" to \"$filePath\"!")
            }
        }
        catch (IOException e) {
            if (closeConnection) {
                try {
                    serv.errorStream?.close()
                }
                catch (Exception ignored) { }
            }

            throw e
        }
        finally {
            if (closeConnection) {
                try {
                    serv.inputStream?.close()
                }
                catch (Exception ignored) { }
                serv.disconnect()
            }
        }

        return res
    }

    /**
     * Ping host
     * @param host host name
     * @param port port number
     * @param timeout timeout in ms
     * @return ping result
     */
    static Boolean PingHost(String host, int port, int timeout) {
        def i = 0
        def res = false
        while (true) {
            def socket = new Socket()
            try {
                socket.connect(new InetSocketAddress(host, port), timeout)
                res = socket.connected
                break
            } catch (IOException ignored) {
                i++
                if (i > 1) {
                    res = false
                    break
                }

                Getl.pause(timeout)
            }
            finally {
                socket.close()
            }
        }

        return res
    }
}