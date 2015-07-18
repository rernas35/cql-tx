/**
 * New node file
 */


var cassandra = require('cassandra-driver');
var async = require('async');
//var NodeCache = require( "node-cache" );
var sqlStrUtility = require('./sql-str-utility');
var cassandraDDLHandler= require('./cassandra-ddl-handler');
var cassandraBase = require('./cassandra-base');
var logger = require("./logger");


function CassandraDMLHandler(){
	

	this.updateTransactional=function(cql,txObject){
		var updateStatement = sqlStrUtility.SqlStrUtility().getUpdateStatement(cql);
		updateStatement.txObject=txObject;
		this.updateTxRecord(updateStatement,txObject);
	}

	this.updateTxRecord=function(statement,txObject){
		logger.debug('inserting tx record on table ' + statement.table);
		var columnClause = '';
		var valueClause = '';
		for (var i=0; i<statement.columns.length;i++){
			var c = statement.columns[i];
			if (columnClause != '')
				columnClause += ',';
			columnClause += c.column;
			if (valueClause != '')
				valueClause += ',';
			valueClause += c.value;
		}
		var changesClause = '';
		for (var i=0; i<statement.changes.length;i++){
			var c = statement.changes[i];
			if (changesClause != '')
				changesClause += ',';
			changesClause += c.column;
		}
		
		this.execute('insert into tx_' + statement.table + '(' + columnClause + ',' + cassandraDDLHandler.getInstance().transactionColumns.getColumnString() + 
				') values(' + valueClause + ',' + cassandraDDLHandler.getInstance().transactionColumns.updateValues + ",'" + changesClause +  "'," + statement.txObject.getTransactionId() +')',
				[],
				statement,
				this.dummyCallback,
				this,
				txObject);
		logger.debug('inserted tx record on table ' + statement.table);
	}
	
	this.insertTransactional=function(cql,txObject){
		var insertStatement = sqlStrUtility.SqlStrUtility().getInsertStatement(cql);
		insertStatement.txObject=txObject;
		this.insertTxRecord(insertStatement,txObject);
	}
	
	this.insertTxRecord=function(statement,txObject){
		logger.debug('inserting tx record on table ' + statement.table);
		var columnClause = '';
		var valueClause = '';
		for (var i=0; i<statement.columns.length;i++){
			var c = statement.columns[i];
			if (columnClause != '')
				columnClause += ',';
			columnClause += c.column;
			if (valueClause != '')
				valueClause += ',';
			valueClause += c.value;
		}
		this.execute('insert into tx_' + statement.table + '(' + columnClause + ',' + cassandraDDLHandler.getInstance().transactionColumns.getColumnString() + 
				') values(' + valueClause + ',' + cassandraDDLHandler.getInstance().transactionColumns.insertValues + ',' + statement.txObject.getTransactionId() +')',
				[],
				statement,
				this.dummyCallback,
				this,
				txObject);
		logger.debug('inserted tx record on table ' + statement.table);
	}
	
	this.dummyCallback=function(rows,statement,thus,txObject){	
		logger.debug("Dummy return from Cassandra DML");
		txObject.txCallback(rows, statement);
	};
	
	
	
	
	
}




module.exports = {
		getInstance : function(){
			CassandraDMLHandler.prototype = cassandraBase.getInstance();
			var handler = new CassandraDMLHandler();
			return {
				executeTransactional : function(cql,txObject){
					if (sqlStrUtility.SqlStrUtility().isUpdate(cql)){
						handler.updateTransactional(cql,txObject);
					}else if (sqlStrUtility.SqlStrUtility().isInsert(cql)){
						handler.insertTransactional(cql,txObject);
					} 
					
				}
			}
		}
		
}

