package getl.files.sub

class Filter implements FilenameFilter {
    Filter (Closure code) {
		this.code = code 
	}

	private Closure code
	
	@Override
	boolean accept(File file, String name) {
		code.call(file, name)
	}
}
