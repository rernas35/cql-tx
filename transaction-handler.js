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
	
	
	this.execute=function(cql,sessionId,callback){
		var txObject = new TxObject(sessionId,cql);
		txObject.txCallback = callback;
		cassandraBase.getInstance().execute('select count(*) as e from TX_TRANSACTIONS',[],null,this.retrieveSessionIdCallback,this,txObject);
		
	}
	
	this.retrieveSessionIdCallback=function(rows,statement,thus,txObject){
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
	
	this.openTransaction=function(openTransactionCallback){
		this.openTransactionCallback = openTransactionCallback;
		this.txCallback4CreateTransaction(uuid.v4());
	} ;
	
	this.commitTransaction=function(uuid, callback){
		var session = sessionHandler.getInstance(uuid);
		session.commitTransaction();
		session.callback = function(){};
		var txObject = new TxObject(uuid,'');
		txObject.txCallback = callback;
		var boundCallback = this.commitTransactionStatusUpdateCallback.bind(this);
		cassandraBase.getInstance().execute1('update TX_TRANSACTIONS set status=2 where txid = ?',[uuid],null,boundCallback,txObject);
	};
	
	this.commitTransactionStatusUpdateCallback=function(rows,statement,txObject){
		txObject.txCallback();
	}
	
	this.retrieveTablesCallback=function(rows,statement,thus){
		if (rows.length()>0){
			for (var i=index;i<rows.length();i++){
				var tableName = rows[i].table_name;
				
			}
			
		}
		
		
	};
	
	this.rollbackTransaction=function(){
		cassandraBase.getInstance().execute('update TX_TRANSACTIONS set status=3 where txid = ?',[uuid],null,this.dummyCallback,this);
	};
	
	this.txCallback4CreateTransaction=function(genSessionId){
		var boundCallback = this.txCallback4CreateTransaction2.bind(this);
		this.sessionId=genSessionId;
		cassandraBase.getInstance().execute1('insert into TX_TRANSACTIONS(txId,start_date,status) values(?,dateof(now()),?)',[genSessionId,1],null,boundCallback);
	};
	
	this.txCallback4CreateTransaction2=function(rows,statement,thus,sessionId){
		var txObject = new TxObject(sessionId);
		txObject.txCallback=function(rows,statement){
			logger.info(this.getTransactionId() + " transaction is created.");
		};
		this.openTransactionCallback();
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