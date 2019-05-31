package getl.lang.opts

import getl.stat.ProcessTime

class ProfileSpec extends BaseSpec {
    final ProcessTime statistics = new ProcessTime()

    /** Start profiling process */
    void startProfile() { statistics.start = new Date() }
    /** Finish profiling process */
    void finishProfile() { statistics.finish(statistics.countRow) }

    /** Profile name */
    String getProfileName() { statistics.name }
    /** Profile name */
    void setProfileName(String value) { statistics.name = value }

    /** Count processed row */
    Integer getCountRow() { statistics.countRow }
    /** Count processed row */
    void setCountRow(Integer value) { statistics.countRow = value }
}
