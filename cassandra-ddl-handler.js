
var async = require('async');
var sqlStrUtility = require('./sql-str-utility');
var cassandraBase = require('./cassandra-base');
var logger = require("./logger");

/*
 * TX-tables : TX_TABLES , TX_COLUMNS ,
 * 
 *  TX_TABLES: table_id, table_name,  creation_date, status
 *  create table TX_TABLES (table_id UUID primary key,table_name text,creation_date timestamp, status int);
 *  create index table_name on tx_tables(table_name);
 *  TX_COLUMNS : column_id, table_id, type, creation_date, modification_date,  status
 *  create table TX_COLUMNS (table_name text,column_name text,column_type text, is_indexed boolean,key_type text,primary key (table_name,column_name));
 *  create table TX_TRANSACTIONS (txId UUID,status int,start_date timestamp,primary key (txId));
 */

function CassandraDDLHandler(){
	
	
	
	this.additionalColumns = {  columns:[{name:'is_removed',type:'boolean'},
	                                     {name:'action', type:'int' },
	                                    {name:'changeset', type:'text'},
	                                    {name:'trx_id', type:'UUID'}],// insert:1 | update:2 | delete:3
	                                    
	                           generateAddtionalColumnsDDL:function(){
									var additionalColumnDDLPart = '';
									for (var i=0; i<this.columns.length; i++){
										var c = this.columns[i];
										if (additionalColumnDDLPart != '')
											additionalColumnDDLPart += ',';
										additionalColumnDDLPart += c.name + ' ' + c.type;
									}
									return additionalColumnDDLPart;
								},
								getColumnString : function(){
									var retString = '';
									this.columns.forEach(function(c) {
										if (retString != '')
											retString += ',';
										retString += c.name;
									});
									return retString;
							  	},
								insertValues : "false,1,''",
								updateValues : "false,2",
								deleteValues : "true,3"
							}; 
	
	this.insertTable4Metadata=function(cql,txObject){
		var insertStatement = sqlStrUtility.SqlStrUtility().getInsertStatement(cql);
		insertStatement.txObject=txObject;
		this.checkTableExists(insertStatement);
	};
	
	this.addTable2TransactionalContext=function(statement){
		
		this.execute('insert into TX_TABLES(table_id,creation_date,table_name, status) values(uuid(),dateof(now()),\'?\',?)',
				[statement.table,1],statement,this.dummyCallback,this);
		
		logger.debug("Retrieving metadata for " + statement.table);
		this.execute("SELECT * FROM system.schema_columns where keyspace_name = 'mykeyspace' and columnfamily_name = '"+ statement.table +"'",
				[],
				statement,
				this.createTxTable,
				this);
		logger.debug("Retrieving metadata for " + statement.table);
		
		
	};
	
	this.createTxTable=function(rows,statement,thus){
		partitionKeyClause = '';
		clusteringKeyClause = '';
		columnsClause = '';
		for(var i=0 ; i<rows.length ; i++){
			c = rows[i];
			columnsClause += c.column_name + ' ' + thus.getType(c.validator) + ',' ;
			if (c.type == 'partition_key'){
				partitionKeyClause = c.column_name;
			}else if (c.type == 'clustering_key'){
				if (clusteringKeyClause != ''){
					clusteringKeyClause += ',';
				}
				clusteringKeyClause += c.column_name;
			} 
		}
		
		primaryKeyClause = 'primary key(trx_id)';
//		primaryKeyClause = 'primary key(' + partitionKeyClause;
//		if (clusteringKeyClause != ''){
//			primaryKeyClause += ',' + clusteringKeyClause;
//		}
//		primaryKeyClause += ')'; 
	
		thus.execute('create table tx_'+ statement.table +'('+columnsClause + thus.additionalColumns.generateAddtionalColumnsDDL() + ',' + primaryKeyClause +')',
				[],statement,thus.callBack4CreateTable,thus);
		
		thus.createColumns(rows,statement);
		
		
	};
	
	this.createColumns=function(rows,statement){
		for(var i=0 ; i<rows.length ; i++){
			c = rows[i];
			var isIndexed = false; 
			if (c.index_name != null)
				isIndexed = true;
			this.execute("insert into TX_COLUMNS(table_name,column_name,column_type, is_indexed,key_type) values('?','?','?',?,'?')",
					[statement.table,c.column_name,c.validator, isIndexed, c.type],statement,this.dummyCallback,this);
			 
		}
		
	}
	
	
	this.callBack4CreateTable=function(rows,statement,thus){
		logger.debug("Retrieving indexes " + statement.table);
		thus.execute("SELECT * FROM system.schema_columns where keyspace_name = 'mykeyspace' and columnfamily_name = '"+ statement.table +"'",
				[],
				statement,
				thus.createIndex4TxClone,
				thus);
		logger.debug("Retrieving indexes for " + statement.table);
		statement.txObject.txCallback(rows,statement);
	};
	
	this.getType=function(validator){
		if (validator == 'org.apache.cassandra.db.marshal.UTF8Type'){
			return 'text';
		}else if (validator == 'org.apache.cassandra.db.marshal.Int32Type'){
			return 'int';
		}else if (validator == 'org.apache.cassandra.db.marshal.LongType'){
			return 'bigint';
		}
		return "";
	};
	
	this.createIndex4TxClone=function(rows,statement,thus){
		rows.forEach(function(c) {
			logger.info('c.index_name : ' + c.index_name);
			if (c.index_name != null){
				logger.debug('Creating index for ' + c.column_name + " on table " + statement.table );
				thus.execute("create index tx_idx_" + c.column_name + " on tx_" + statement.table + "(" + c.column_name + ")",[],statement,thus.dummyCallback,thus);
				logger.debug('Created index for ' + c.column_name + " on table " + statement.table );
			}
			
		});
		
	};
	
	this.dummyCallback=function(rows,statement,thus){	
		logger.debug("Dummy return from Cassandra DDL");
	};
	
	this.checkTableExists=function(statement){
		logger.debug('Check for table :' + statement.table);
		this.execute('select count(*) as c from TX_TABLES where table_name=\'?\'',
				[statement.table],
				statement,
				this.callback4CheckTableExists,
				this
				);
		
	};
	
	this.callback4CheckTableExists=function(rows,statement,thus){
		if (rows[0].c > 0){
			logger.debug('table already exists!');
			statement.txObject.txCallback(rows,statement);
		}else {
			thus.addTable2TransactionalContext(statement);
		}
	
		
	};
	
	this.updateTable4Metadata=function(cql,txObject){
		var updateStatement = sqlStrUtility.SqlStrUtility().getUpdateStatement(cql);
		updateStatement.txObject=txObject;
		this.checkTableExists(updateStatement);
	};
	
	this.deleteTable4Metadata=function(cql,txObject){
		var deleteStatement = sqlStrUtility.SqlStrUtility().getDeleteStatement(cql);
		deleteStatement.txObject=txObject;
		this.checkTableExists(deleteStatement);
	};
	

	
	
}

module.exports = {
		getInstance : function(){
			CassandraDDLHandler.prototype = cassandraBase.getInstance();
			var handler = new CassandraDDLHandler();
			return {
				process4Metadata : function(cql,txObject){
					if (sqlStrUtility.SqlStrUtility().isUpdate(cql)){
						handler.updateTable4Metadata(cql,txObject);
					}else if (sqlStrUtility.SqlStrUtility().isInsert(cql)){
						handler.insertTable4Metadata(cql,txObject);
					}else if (sqlStrUtility.SqlStrUtility().isDelete(cql)){
						handler.deleteTable4Metadata(cql,txObject);
					} 
				},
				transactionColumns : handler.additionalColumns
			};
		}	
}

