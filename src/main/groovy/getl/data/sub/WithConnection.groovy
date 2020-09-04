package getl.data.sub

import getl.data.Connection

/**
 * Object linking interface with a connection
 * @author Alexsey Konstantinov
 */
interface WithConnection {
    /** Connection */
    Connection getConnection()
    /** Connection */
    void setConnection(Connection value)

    /** Clone object and its connection */
    Object cloneWithConnection()
}
