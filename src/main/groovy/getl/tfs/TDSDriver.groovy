package getl.tfs

import getl.h2.H2Connection
import getl.h2.H2Driver
import groovy.transform.InheritConstructors

@InheritConstructors
class TDSDriver extends H2Driver {
    @Override
    void connect() {
        super.connect()
        def con = connection as H2Connection
        if (con.connectHost == null && con.connectDatabase != null && !con.inMemory) {
            new File(con.connectDatabase + '.mv.db').deleteOnExit()
            new File(con.connectDatabase + '.trace.db').deleteOnExit()
        }
    }
}