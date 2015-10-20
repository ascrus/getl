package getl.data

/**
 GETL - based package in Groovy, which automates the work of loading and transforming data. His name is an acronym for «Groovy ETL».

 GETL is a set of libraries of pre-built classes and objects that can be used to solve problems unpacking,
 transform and load data into programs written in Groovy, or Java, as well as from any software that supports
 the work with Java classes.
 
 Copyright (C) 2013  Alexsey Konstantonov (ASCRUS)

 This program is free software: you can redistribute it and/or modify
 it under the terms of the GNU Lesser General Public License as published by
 the Free Software Foundation, either version 3 of the License, or
 (at your option) any later version.

 This program is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU General Public License for more details.

 You should have received a copy of the GNU General Public License and
 GNU Lesser General Public License along with this program.
 If not, see <http://www.gnu.org/licenses/>.
*/

import java.util.List;

import groovy.transform.InheritConstructors
import getl.utils.*

/**
 * Base virtual dataset class 
 * @author Alexsey Konstantinov
 *
 */
@InheritConstructors
class VirtualDataset extends Dataset {
	VirtualDataset () {
		super()
		
		sysParams.isVirtual = true
	}
	
	/**
	 * Destinition dataset
	 * @return
	 */
	public Dataset getDest () { params.dest }
	public void setDest (Dataset value) { params.dest = value }
	
	@Override
	public List<String> excludeSaveParams () {
		super.excludeSaveParams() + ["dest"]
	}
	
	@Override
	public String getObjectName() { dest.objectName }
	
	@Override
	public String getObjectFullName() { dest.objectFullName }
}
