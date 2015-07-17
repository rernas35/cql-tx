/**
 * New node file
 */

var cassandra = require('cassandra-driver');
var logger = require("./logger");

function CassandraBaseHandler(){
	
	var client = new cassandra.Client({contactPoints: ['127.0.0.1'], keyspace: 'mykeyspace'});
	
	function prepareStatement(cql,parameters){
		for(var i = 0 ; i < parameters.length;i++){
			cql = cql.replace('?', parameters[i]);
		}
		logger.debug("Statement is replaced :"  + cql);
		return cql;
	}
	
	function executeInternal(cql,parameters,statement,callback,thus,txObject){
		logger.debug('csql to be executed :' +  cql);
		logger.debug('parameters :' +  parameters);
		var pcql = prepareStatement(cql,parameters);
		client.execute(pcql,[], function (err, result) {
	           if (!err){
	               if (result.rows && result.rows.length > 0 ) {
	                   logger.debug("result.rows.length : " +  result.rows.length);
	               } else {
	                   logger.debug("No results from the cql : " + cql);
	               }
	               try{
	            	   callback(result.rows,statement,thus,txObject);
	               }catch(ex){
	            	   txObject.errCallback(ex);
	               }
	           }

	       }); 
	}
	
	function executeInternal1(cql,parameters,statement,callback,txObject){
		logger.debug('csql to be executed :' +  cql);
		logger.debug('parameters :' +  parameters);
		var pcql = prepareStatement(cql,parameters);
		client.execute(pcql,[], function (err, result) {
	           if (!err){
	               if (result.rows && result.rows.length > 0 ) {
	                   logger.debug("result.rows.length : " +  result.rows.length);
	               } else {
	                   logger.debug("No results from the cql : " + cql);
	               }
	               try {
	            	   callback(result.rows,statement,txObject);
	               }catch(ex){
	            	   txObject.errCallback(ex);
	               }
	           }

	       }); 
	}
	
	return {execute : function(cql,parameters,statement,callback,thus,txObject){
							executeInternal(cql,parameters,statement,callback,thus,txObject);
						},
			execute1 : function(cql,parameters,statement,callback,txObject){
							executeInternal1(cql,parameters,statement,callback,txObject);
						}
	};
	
}

module.exports = {
		getInstance : function(){
			return new CassandraBaseHandler();
		}	
}
