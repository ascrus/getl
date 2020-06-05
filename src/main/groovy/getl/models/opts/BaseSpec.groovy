/*
 GETL - based package in Groovy, which automates the work of loading and transforming data. His name is an acronym for "Groovy ETL".

 GETL is a set of libraries of pre-built classes and objects that can be used to solve problems unpacking,
 transform and load data into programs written in Groovy, or Java, as well as from any software that supports
 the work with Java classes.

 Copyright (C) EasyData Company LTD

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
package getl.models.opts

import getl.models.sub.BaseModel

class BaseSpec extends getl.lang.opts.BaseSpec {
    BaseSpec(BaseModel model) {
        super(model)
    }

    BaseSpec(BaseModel model, Map importParams) {
        super(model, false, importParams)
    }

    @Override
    protected void initSpec() {
        super.initSpec()
        params.objectVars = [:] as Map<String, Object>
    }

    /** Owner model */
    BaseModel getOwnerModel() { ownerObject as BaseModel }

    /** Model object variables */
    Map<String, Object> getObjectVars() { params.objectVars as Map<String, Object> }
    /** Model object variables */
    void setObjectVars(Map<String, Object> value) {
        objectVars.clear()
        if (value != null)
            objectVars.putAll(value)
    }
}