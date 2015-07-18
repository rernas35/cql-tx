/**
 * New node file
 */


var uuid = require("node-uuid");

var ddlHandler = require('./cassandra-ddl-handler');
var dmlHandler = require('./cassandra-dml-handler');
var cassandraBase = require('./cassandra-base');
var sessionHandler = require('./session');
var logger = require("./logger");


function TxObject(transactionId,cql) {
	var retObject = Object.create(null);
	
	retObject.setTransactionId=function(transactionId){
		this.txId = transactionId;
	}
	retObject.getTransactionId=function(){
		return this.txId;
	}
	retObject.setCql=function(cql){
		this.cql = cql;
	}
	retObject.getCql=function(){
		return this.cql;
	}
	retObject.setStatement=function(statement){
		this.statement=statement;
	}
	retObject.getStatement=function(){
		return this.statement;
	}
	
	retObject.setTransactionId(transactionId);
	retObject.setCql(cql);
	return retObject;

}

function TransactionHandler(){
	
	
	this.execute=function(cql,sessionId,callback,errCallback){
		var txObject = new TxObject(sessionId,cql);
		txObject.txCallback = callback;
		txObject.errCallback = errCallback;
		try {
			var boundCallback = this.retrieveSessionIdCallback.bind(this);
			cassandraBase.getInstance().execute1('select count(*) as e from TX_TRANSACTIONS',[],null,boundCallback,txObject);
		}catch (ex){
			txObject.errCallback(ex);
		}
		
	}
	
	this.retrieveSessionIdCallback=function(rows,statement,txObject){
		if (rows[0].e > 0){
			var callbackImpl = txObject.txCallback;
			txObject.txCallback=function(rows, statement){
				logger.debug("execute transactional DDL has worked....");
				var txObject4dml = new TxObject(txObject.getTransactionId(),txObject.getCql());
				txObject4dml.txCallback=callbackImpl;
				dmlHandler.getInstance().executeTransactional(txObject4dml.getCql(),txObject4dml);
			}
			ddlHandler.getInstance().process4Metadata(txObject.getCql(),txObject);
			
			
		}else {
			
		}
	}
	
	this.openTransaction=function(openTransactionCallback,errCallback){
		var txObject = new TxObject(uuid.v4(),'');
		txObject.txCallback=openTransactionCallback;
		txObject.errCallback = errCallback;
		try {
			this.txCallback4CreateTransaction(txObject);
		}catch (ex){
			errCallback(ex);
		}
	} ;
	
	this.commitTransaction=function(uuid, callback,errCallback){
		var txObject = new TxObject(uuid,'');
		txObject.txCallback = callback;
		txObject.errCallback = errCallback;
		try {
			var session = sessionHandler.getInstance(txObject);
			session.commitTransaction();
		}catch (ex){
			txObject.errCallback(ex);
		}
	};
	
	
	
	this.retrieveTablesCallback=function(rows,statement,thus){
		if (rows.length()>0){
			for (var i=index;i<rows.length();i++){
				var tableName = rows[i].table_name;
				
			}
			
		}
		
		
	};
	
	this.rollbackTransaction=function(uuid,callback,errCallback){
		try{
			cassandraBase.getInstance().execute('update TX_TRANSACTIONS set status=3 where txid = ?',[uuid],null,this.dummyCallback,this);
		}catch (ex){
			errCallback(ex);
		}

	};
	
	this.txCallback4CreateTransaction=function(txObject){
		var boundCallback = this.txCallback4CreateTransaction2.bind(this);
		cassandraBase.getInstance().execute1('insert into TX_TRANSACTIONS(txId,start_date,status) values(?,dateof(now()),?)',[txObject.getTransactionId(),1],null,boundCallback,txObject);
	};
	
	this.txCallback4CreateTransaction2=function(rows,statement,txObject){
		txObject.txCallback();
	};
	
	this.dummyCallback=function(rows,statement,thus){	
		logger.debug("Dummy return from Cassandra Transaction");
	};
	
}


module.exports = {
		getInstance : function(){
			return new TransactionHandler();
		}	
}