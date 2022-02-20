package getl.xml.sub

/**
 * XML parser without error messages
 * @author Alexsey Konstantinov
 */
class XmlParser extends groovy.xml.XmlParser {
    XmlParser(Boolean quietMode = true) {
        super()
        setErrorHandler(new QuietErrorHandler(quietMode))
    }
}