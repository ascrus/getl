package getl.deploy

interface VersionInfo {
    /** GETL version */
    String getVersion()

    /** GETL version as numeric */
    BigDecimal getVersionNum()

    /** Compatibility GETL version */
    BigDecimal getVersionNumCompatibility()

    /** Years development */
    String getYears()
}