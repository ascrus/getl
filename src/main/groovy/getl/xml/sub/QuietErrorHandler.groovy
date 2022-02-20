package getl.xml.sub

import getl.utils.Logs
import groovy.transform.CompileStatic
import org.xml.sax.ErrorHandler
import org.xml.sax.SAXException
import org.xml.sax.SAXParseException

/**
 * Quiet error handler for XML parser
 * @author Alexsey Konstantinov
*/
@CompileStatic
class QuietErrorHandler implements ErrorHandler {
    QuietErrorHandler(Boolean quietMode = true) {
        this.quietMode = quietMode
    }

    private Boolean quietMode

    @Override
    void warning(SAXParseException exception) throws SAXException {
        if (!quietMode && Logs.global != null)
            Logs.Warning("Warning XMLParser: ${exception.message}")
    }

    @Override
    void error(SAXParseException exception) throws SAXException {
        if (!quietMode && Logs.global != null)
            Logs.Severe("Error XMLParser: ${exception.message}")
    }

    @Override
    void fatalError(SAXParseException exception) throws SAXException {
        if (!quietMode && Logs.global != null)
            Logs.Severe("Fatal error XMLParser: ${exception.message}")
    }
}