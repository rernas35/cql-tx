/**
 * New node file
 */


var cassandra = require('cassandra-driver');
var config = require("./config");
var async = require("async");


var client = new cassandra.Client(config.cassandraClient);

function init(){
	async.series([
	              execute('create table TX_TABLES (table_name text primary key,  creation_date timestamp,status int)',errFunction),
	              execute('create table TX_COLUMNS (table_name text,column_name text,column_type text, is_indexed boolean,key_type text,primary key (table_name,column_name));',errFunction),
	              execute('create table TX_TRANSACTIONS (txId UUID,status int,start_date timestamp,primary key (txId));',errFunction),
	              execute('create table TX_COMPLETED_TRANSACTIONS (txId UUID,status int,start_date timestamp,primary key (txId));',errFunction),
	              execute('CREATE INDEX tx_date_idx ON TX_TRANSACTIONS (start_date);',errFunction)
	          ]);
	
}	

function errFunction(ex){
	console.log(ex.message);
}

function execute(cql,errFunction){
	client.execute(cql, function (err, result) {
        if (err){
        	errFunction(err);
        }

    }); 
}

init();