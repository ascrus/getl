package getl.lang.opts

import getl.utils.BoolUtils

class LangSpec extends BaseSpec {
    public boolean getProcessTimeTracing() { BoolUtils.IsValue(params.processTimeTracing) }
    public void setProcessTimeTracing(boolean value) {params.processTimeTracing = value }
}