package getl.data.sub

/**
 * File write options
 * @author Aleksey Konstantinov
 */
class FileWriteOpts {
    /** Number part */
    public Integer partNumber
    /** File name */
    public String fileName
    /** Temporary file name */
    public String tempFileName
    /** Count write rows */
    public Long countRows = 0
    /** Append to exist file */
    public Boolean append
    /** Delete file is empty */
    public Boolean deleteOnEmpty
    /** Code page file */
    public String encode
    /** File is saved */
    public Boolean readyFile = false
}