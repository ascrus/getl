package getl.files.sub

import getl.files.Manager

/**
 * Resource manager catalog item
 * @author Alexsey Konstantinov
 */
class ResourceCatalogElem {
    public String filename
    public String filepath
    public Long filedate
    public Long filesize
    public Manager.TypeFile type
    public ResourceCatalogElem parent
    public List<ResourceCatalogElem> files
}