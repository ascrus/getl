package getl.lang.opts

import getl.lang.Getl
import getl.stat.ProcessTime

/**
 * Profiler options
 * @author Alexsey Konstantinov
 *
 */
class ProfileSpec extends BaseSpec {
    ProfileSpec(Object owner, String name, String objectName = null, Boolean isProfile = false) {
        super(owner)
        this.isProfile = isProfile
        statistics = new ProcessTime(
                dslCreator: owner as Getl,
                name: name,
                logLevel: (owner as Getl).options.processTimeLevelLog,
                debug: ((!isProfile)?(owner as Getl).options.processTimeDebug:false),
                objectName: objectName?:'row',
                abbrName: (!isProfile)?'STAT':null
        )
    }

    /** Getl owner */
    private Getl getGetl() { ownerObject as Getl }

    /** Getl profile block */
    private Boolean isProfile = false
    /** Getl profile block */
    Boolean getIsProfile() { isProfile }

    /** Profile statistics object */
    private ProcessTime statistics
    /** Profile statistics object */
    ProcessTime getStatistics() { statistics }

    /** Start profiling process */
    void startProfile() {
        statistics.clear()
        statistics.countRow = 0
    }

    /** Finish profiling process */
    void finishProfile() {
        statistics.finish(statistics.countRow)
    }

    /** Profile name */
    String getProfileName() { statistics.name }
    /** Profile name */
    void setProfileName(String value) { statistics.name = value }

    /** Count processed row */
    Long getCountRow() { statistics.countRow }
    /** Count processed row */
    void setCountRow(Long value) { statistics.countRow = value }
}