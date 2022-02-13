package getl.utils

import getl.exception.ExceptionGETL
import groovy.transform.CompileStatic
import groovy.transform.NamedVariant

import java.nio.charset.StandardCharsets
import java.time.format.DateTimeFormatter

/**
 * Functions for working with web connections
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
     * @param service web service name
     * @param connectTimeout connection timeout in ms
     * @param readTimeout read timeout in ms
     * @param requestMethod request method (GET or POST)
     * @param params connection parameters
     * @param vars variables for parameters
     * @return created connection to web service
     */
    @NamedVariant
    static HttpURLConnection CreateConnection(String url, String service = null, Integer connectTimeout = null, Integer readTimeout = null,
                                       String requestMethod = null, Map<String, Object> params = null, Map<String, Object> vars = null) {
        if (requestMethod == null)
            throw new ExceptionGETL('Required "requestMethod" parameter value!')
        if (!(requestMethod in [WEBREQUESTMETHODGET, WEBREQUESTMETHODPOST]))
            throw new ExceptionGETL("Unknown request method \"$requestMethod\"!")

        if (service != null) {
            url += ((url[url.length() - 1] != '/')?'/':'') + service
        }

        def headers = [:] as Map<String, String>

        if (params != null && !params.isEmpty()) {
            def list = [] as List<String>
            def isVars = (vars != null && !vars.isEmpty())
            params.each { k, v ->
                def isHeader = k.matches('(?i)^header[.].+')
                if (isHeader)
                    k = k.substring(7)

                if (v == null || (v == '')) {
                    if (!isHeader)
                        list.add(k)

                    return
                }

                if ((v instanceof String || v instanceof GString) && isVars) {
                    v = StringUtils.EvalMacroString(v.toString(), vars, true) {
                        (it instanceof Date)?(UrlDateFormatter.format((it as Date).toLocalDateTime()) + 'Z'):
                                URLEncoder.encode(it.toString(), StandardCharsets.UTF_8.toString())
                    }
                }

                String val
                if (v instanceof Date)
                    val = UrlDateFormatter.format((v as Date).toLocalDateTime()) + 'Z'
                else
                    val = v

                if (!isHeader)
                    list.add(k + '=' + val)
                else
                    headers.put(k, val)
            }
            url += '?' + list.join('&')
        }

        def serv = new URL(url).openConnection() as HttpURLConnection
        if (connectTimeout != null)
            serv.connectTimeout = connectTimeout
        if (readTimeout != null)
            serv.readTimeout = readTimeout
        serv.requestMethod = requestMethod
        headers.each { key, value ->
            serv.setRequestProperty(key, value)
        }

        return serv
    }

    /**
     * Read web service data
     * @param connection web service connection
     * @param filePath path to save data to file
     * @param closeConnection close the connection when finished
     * @return web service response code
     */
    static Integer DataToFile(HttpURLConnection serv, String filePath, Boolean closeConnection = true) {
        Integer res = null

        try {
            serv.tap {
                res = responseCode
                if (res != HTTP_OK)
                    throw new ExceptionGETL("Error $res reading service \"${serv.URL.text}\": $responseMessage")

                def tn = filePath + '.getltemp'
                def tFile = new File(tn)
                tFile.createNewFile()
                tFile.append(inputStream)

                def oFile = new File(filePath)
                if (oFile.exists())
                    oFile.delete()
                if (!tFile.renameTo(oFile))
                    throw new ExceptionGETL("Can not rename file $tn to $filePath!")

                return true
            }
        }
        finally {
            if (BoolUtils.IsValue(closeConnection))
                serv.disconnect()
        }

        return res
    }
}