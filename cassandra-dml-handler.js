/**
 * New node file
 */


var cassandra = require('cassandra-driver');
var async = require('async');
var NodeCache = require( "node-cache" );
var sqlStrUtility = require('./sql-str-utility');
var cassandraDDLHandler= require('./cassandra-ddl-handler');
var cassandraBase = require('./cassandra-base');
var logger = require("./logger");

/*
 * TX-tables : TX_TABLES , TX_COLUMNS ,
 * 
 *  TX_TABLES: table_id, table_name,  creation_date, status
 *  create table TX_TABLES (table_id UUID primary key,table_name text,creation_date timestamp, status int);
 *  create index table_name on tx_tables(table_name);
 *  TX_COLUMNS : column_id, table_id, type, creation_date, modification_date,  status
 *  create table TX_COLUMNS (column_id UUID primary key, table_id int,column_name text,creation_date timestamp,modifition_date timestamp, status int);
 */

function CassandraDMLHandler(){
	

	this.updateTransactional=function(cql){
		//TODO : update transactional must be implemented.
	}
	
	this.insertTransactional=function(cql,txId){
		var insertStatement = sqlStrUtility.SqlStrUtility().getInsertStatement(cql);
		this.insertTxRecord(insertStatement,txId);
	}
	
	this.insertTxRecord=function(statement,txId){
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
				') values(' + valueClause + ',' + cassandraDDLHandler.getInstance().transactionColumns.insertValues + ',' + txId +')',
				[],
				statement,
				this.dummyCallback,
				this);
		logger.debug('inserted tx record on table ' + statement.table);
	}
	
	this.dummyCallback=function(rows,statement,thus){	
		logger.debug("Dummy return from Cassandra DML");
	};
	
	
	
	
	
}




module.exports = {
		getInstance : function(){
			CassandraDMLHandler.prototype = cassandraBase.getInstance();
			var handler = new CassandraDMLHandler();
			return {
				executeTransactional : function(cql,txId){
					if (sqlStrUtility.SqlStrUtility().isUpdate(cql)){
						handler.updateTransactional(cql,txId);
					}else if (sqlStrUtility.SqlStrUtility().isInsert(cql)){
						handler.insertTransactional(cql,txId);
					} 
					
				}
			}
		}
		
}

