package getl.files.sub

import net.lingala.zip4j.model.ExcludeFileFilter

class Filter implements FilenameFilter, ExcludeFileFilter {
    Filter(Closure filterCode) {
		this.code = filterCode
	}

	private Closure<Boolean> code
	
	@Override
	boolean accept(File file, String name) {
		return code(file, name)
	}

	@Override
	boolean isExcluded(File file) {
		return !code(file.parentFile, file.name)
	}
}