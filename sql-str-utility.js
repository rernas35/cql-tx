/**
 * New node file
 */
var S = require('string');
var logger = require("./logger");

function KeyValue(column, value) {
	var retObject = Object.create(null);
	
	retObject.setValue = function(value){
		this.value = value;
		if (S(value).startsWith('\''))
			this.type = 'string'
		else
			this.type = 'numeric'
	}
	
	retObject.column = column;
	retObject.setValue(value);
	
	return retObject;

}

function SqlUtility() {

	var updateRegex = /update(?:\s+)(\S+)(?:\s+)set(?:\s+)(.+)(?:\s+)where(?:\s+)(.+)(?:\s*)/ig;
	var updateSetterRegex = /(\S+)=(\'*.+\'*)/;

	var insertRegex = /insert(?:\s+)into(?:\s+)(\S+)\((.+)\)(?:\s+)values\((.+)\)(?:\s*)/ig;
	
	var deleteRegex = /delete(?:\s+)from(?:\s+)(\S+)(?:\s+)where(?:\s+)(.+)(?:\s*)/ig;

	var selectRegex = "select bisi bisi";

	function getSelectColumns(selectSql) {

	}

	function getSetter(setterSql) {
		var retArray = [];
		var setters = setterSql.split(',');
		setters.forEach(function(entry) {
			var trimmed = S(entry).trim();
			var setterMatching = updateSetterRegex.exec(trimmed);
			retArray.push(new KeyValue(setterMatching[1], setterMatching[2]));
		});
		return retArray;
	}
	
	function getCriteria(setterSql) {
		var retArray = [];
		var setters = setterSql.split(/and|AND|And|aNd|anD|AnD|or|Or|oR|OR/);
		setters.forEach(function(entry) {
			var trimmed = S(entry).trim();
			var setterMatching = updateSetterRegex.exec(trimmed);
			retArray.push(new KeyValue(setterMatching[1], setterMatching[2]));
		});
		return retArray;
	}


	function getInsertKeyValues(columns, values) {
		var retArray = [];
		var carray = columns.split(',');
		var varray = values.split(',');
		for (var i1 = 0; i1 < carray.length; i1++) {
			retArray.push(new KeyValue(S(carray[i1]).trim().s, S(varray[i1])
					.trim().s));
		}
		return retArray;
	}
	
	function matchWithParameters(keyValueArray,parameters){
		var keyValueIndex=0;
		var parameterIndex=0;
		for (keyValueIndex=0;keyValueIndex<keyValueArray.length;keyValueIndex++){
			var entry = keyValueArray[keyValueIndex];
			if (entry.value == '?'){
				entry.setValue(parameters[parameterIndex]);
				parameterIndex++;
			}
		}
	}

	function getColumnString(columns){
		var retString = '';
		columns.forEach(function(c) {
			if (retString != '')
				retString += ',';
			retString += c.column;
		});
		return retString;
	}
	
	function getUniqueColumnList(columns){
		var retArray = [];
		columns.forEach(function(c) {
			if (retArray.indexOf("asdsa") == -1){
				retArray.push(c.column)
			}
		});
		return retArray;
	}
	
	return {
		
		isUpdate : function(cql){
			var matching = updateRegex.exec(cql);
			if (matching != null &&  matching.length > 0){
				return true;
			}
		},
		
		isInsert : function(cql){
			var matching = insertRegex.exec(cql);
			if (matching != null &&  matching.length > 0){
				return true;
			}
		},
		isDelete : function(cql){
			var matching = deleteRegex.exec(cql);
			if (matching != null &&  matching.length > 0){
				return true;
			}
		},

	
		///update(?:\s+)(\S+)(?:\s+)set(?:\s+)(.+)(?:\s+)where(?:\s+)(.+)(?:\s*)/ig;
		getUpdateStatement : function(updateSql, parameters) {
			var matching = updateRegex.exec(updateSql);
			var setArray = getSetter(matching[2]);
			var retArray = []
			setArray.forEach(function(entry) {
				retArray.push(entry);
			});

			var criteriaArray = getCriteria(matching[3]);
			criteriaArray.forEach(function(entry) {
				retArray.push(entry);
			});
			
			if (parameters != undefined){
				matchWithParameters(retArray,parameters)
			}
				
			
			return {table: matching[1] ,
					columns: retArray,
					criterias : criteriaArray,
					changes : setArray,
					uniqueColumns:getUniqueColumnList(retArray),
					columnString: getColumnString(retArray)};
		},
		
		///delete(?:\s+)from(?:\s+)(\S+)(?:\s+)where(?:\s+)(.+)(?:\s*)/ig;
		getDeleteStatement : function(deleteSql, parameters) {
			var matching = deleteRegex.exec(deleteSql);
			var retArray = []

			var criteriaArray = getCriteria(matching[2]);
			criteriaArray.forEach(function(entry) {
				retArray.push(entry);
			});
			
			if (parameters != undefined){
				matchWithParameters(retArray,parameters)
			}
				
			
			return {table: matching[1] ,
					columns: retArray,
					criterias : criteriaArray,
					uniqueColumns:getUniqueColumnList(retArray),
					columnString: getColumnString(retArray)};
		},

		getInsertStatement : function(insertSql,parameters) {
			var matching = insertRegex.exec(insertSql);
			var columnArray = getInsertKeyValues(matching[2], matching[3]);
			var retArray = [];
			columnArray.forEach(function(entry) {
				retArray.push(entry);
			});
			if (parameters != undefined){
				matchWithParameters(retArray,parameters)
			}
			return {table: matching[1],
					columns: retArray,
					criterias : [],
					uniqueColumns:getUniqueColumnList(retArray),
					columnString: getColumnString(retArray)};
		}
		
		
		
	}}



module.exports = {
		SqlStrUtility : function(){
			return new SqlUtility();
		}
		
}
