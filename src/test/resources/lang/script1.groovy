@Field String fileName

void check() {
    assert fileName != null
}

csvConnection('con1', true) { }
csv('file1', true) {
    useConnection csvConnection('con1')
    fileName = this.fileName
}
