package getl.impala.opts

import getl.jdbc.opts.WriteSpec
import getl.utils.BoolUtils
import groovy.transform.InheritConstructors

/**
 * Options for writing Impala table
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
class ImpalaWriteSpec extends WriteSpec {
    /** Replace data in table then insert */
    Boolean getOverwrite() { BoolUtils.IsValue(params.overwrite) }
    /** Replace data in table then insert */
    void setOverwrite(Boolean value) { params.overwrite = value }

    static final String snappyCompressionCodec = 'snappy'
    static final String gzipCompressionCodec = 'gzip'
    static final String noneCompressionCodec = 'none'

    /** Compression codec */
    String getCompression() { params.compression as String }
    /** Compression codec */
    void setCompression(String value) { params.compression = value }
}