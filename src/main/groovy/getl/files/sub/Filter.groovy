package getl.files.sub

class Filter implements FilenameFilter {
	Closure code

    Filter (Closure code) {
		this.code = code 
	}
	
	@Override
	boolean accept(File file, String name) {
		code.call(file, name)
	}
}
