
var cassandra = require('cassandra-driver');
var async = require('async');
var NodeCache = require( "node-cache" );
var sqlStrUtility = require('./sql-str-utility');
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

function CassandraDDLHandler(){
	var client = new cassandra.Client({contactPoints: ['127.0.0.1'], keyspace: 'mykeyspace'});
	var txCache = new NodeCache( { stdTTL: 100, checkperiod: 120 } );
	
	var additionalColumns = {  columns : [{name:'is_removed',type:'boolean'},
	                                    {name:'changeset', type:'set'},
	                                    {name:'action', type :'int' }],// insert:1 | update:2 | delete:3
	                                    
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
										retString += c.column;
									});
									return retString;
							  	},
								insertValues : 'false,{},1'
							}; 
	
	function prepareStatement(cql,parameters){
		for(var i = 0 ; i < parameters.length;i++){
			cql = cql.replace('?', parameters[i]);
		}
		logger.debug("Statement is replaced :"  + cql);
		return cql;
	}
	
	function execute(cql,parameters,statement,callback){
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
	               callback(result.rows,statement);
	           }

	       }); 
	}
	
	function insertTable4Metadata(cql){
		var insertStatement = sqlStrUtility.SqlStrUtility().getInsertStatement(cql);
		checkTableExists(insertStatement);
	}
	
	function addTable2TransactionalContext(statement){
		
		execute('insert into TX_TABLES(table_id,creation_date,table_name, status) values(uuid(),dateof(now()),\'?\',?)',
				[statement.table,1],statement,dummyCallback);
		
		logger.debug("Retrieving metadata for " + statement.table);
		execute("SELECT * FROM system.schema_columns where keyspace_name = 'mykeyspace' and columnfamily_name = '"+ statement.table +"'",
				[],
				statement,
				createTxTable)
		logger.debug("Retrieving metadata for " + statement.table);
		
		
	}
	
	function createTxTable(rows,statement){
		partitionKeyClause = '';
		clusteringKeyClause = '';
		columnsClause = '';
		for(var i=0 ; i<rows.length ; i++){
			c = rows[i];
			columnsClause += c.column_name + ' ' + getType(c.validator) + ',' ;
			if (c.type == 'partition_key'){
				partitionKeyClause = c.column_name;
			}else if (c.type == 'clustering_key'){
				if (clusteringKeyClause != ''){
					clusteringKeyClause += ',';
				}
				clusteringKeyClause += c.column_name;
			} 
		}
		
		primaryKeyClause = '';
		primaryKeyClause = 'primary key(' + partitionKeyClause;
		if (clusteringKeyClause != ''){
			primaryKeyClause += ',' + clusteringKeyClause;
		}
		primaryKeyClause += ')'; 
	
		execute('create table tx_'+ statement.table +'('+columnsClause + additionalColumns.generateAddtionalColumnsDDL() + ',' + primaryKeyClause +');',
				[],statement,callBack4CreateTable);
		
	}
	
	function callBack4CreateTable(rows,statement){
		logger.debug("Retrieving indexes " + statement.table);
		execute("SELECT * FROM system.schema_columns where keyspace_name = 'mykeyspace' and columnfamily_name = '"+ statement.table +"'",
				[],
				statement,
				createIndex4TxClone);
		logger.debug("Retrieving indexes for " + statement.table);
		
	}
	
	function getType(validator){
		if (validator == 'org.apache.cassandra.db.marshal.UTF8Type'){
			return 'text';
		}else if (validator == 'org.apache.cassandra.db.marshal.Int32Type'){
			return 'int';
		}
		return "";
	}
	
	function createIndex4TxClone(rows,statement){
		rows.forEach(function(c) {
			logger.info('c.index_name : ' + c.index_name);
			if (c.index_name != null){
				logger.debug('Creating index for ' + c.column_name + " on table " + statement.table );
				execute("create index tx_idx_" + c.column_name + " on tx_" + statement.table + "(" + c.column_name + ")",[],statement,dummyCallback);
				logger.debug('Created index for ' + c.column_name + " on table " + statement.table );
			}
			
		});
	}
	
	function dummyCallback(rows,statement){	
		logger.debug("Dummy return from Cassandra");
	}
	
	function checkTableExists(statement){
		logger.debug('Check for table :' + statement.table);
		execute('select count(*) as c from TX_TABLES where table_name=\'?\'',
				[statement.table],
				statement,
				function(rows,statement){
					if (rows[0].c > 0){
						logger.debug('table already exists!');
					}else {
						addTable2TransactionalContext(statement);
					}
				}
				);
		
	}
	
	function updateTable4Metadata(cql){
		var insertStatement = sqlStrUtility.SqlStrUtility().getUpdateStatement(cql);
		checkTableExists(insertStatement);
	}

	
	function updateTransactional(cql){
		//TODO : update transactional must be implemented.
	}
	
	function insertTransactional(cql){
		
		
		
	}
	
	function insertTxRecord(statement){
		logger.debug('inserting tx record on table ' + statement.table);
		var columnClause = '';
		var valueClause = '';
		for (var i=0; i<statement.columns.length;i++){
			var c = statement.columns[i];
			if (columnClause != '')
				columnClause += ',';
			columnStatement += c.column;
			if (valueClause != '')
				valueClause += ',';
			valueStatement += c.value;
		}
		execute('insert into ' + statement.table + '(' + columnClause + ',' + additionalColumns.getColumnString() + ' values(' + valueClause + ',' + additionalColumns.insertValues + ')',
				[],
				statement,
				dummyCallback);
		
		
		
		logger.debug('inserted tx record on table ' + statement.table);
	}
	
	
	return {
		process4Metadata : function(cql){
			if (sqlStrUtility.SqlStrUtility().isUpdate(cql)){
				updateTable4Metadata(cql);
			}else if (sqlStrUtility.SqlStrUtility().isInsert(cql)){
				insertTable4Metadata(cql);
			} 
		},
		
		executeTransactional : function(cql){
			if (sqlStrUtility.SqlStrUtility().isUpdate(cql)){
				updateTransactional(cql);
			}else if (sqlStrUtility.SqlStrUtility().isInsert(cql)){
				insertTransactional(cql);
			} 
			
		}
		
		
		
	}
	
}

//CassandraDDLHandler().process4Metadata('insert into T1(sudur,budur) values(12,23)');

CassandraDDLHandler().process4Metadata('update users set fname=12 where lname=23');

module.exports = {
		CassandraDDLHandler : function(){
			return new CassandraDDLHandler();
		}	
}

