/**
 * New node file
 */


var ddlHandler = require('./cassandra-ddl-handler');
var dmlHandler = require('./cassandra-dml-handler');
var cassandraBase = require('./cassandra-base');
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
	
	
	this.execute=function(cql,sessionId){
		ddlHandler.process4Metadata(cql);
		dmlHandler.executeTransactional(cql);
		
	}
	
	
	
	
	this.openTransaction=function(){
		cassandraBase.getInstance().execute('select uuid() as sessionId from TX_TABLES',[],null,this.txCallback4CreateTransaction,this);
		
	} ;
	
	this.commitTransaction=function(uuid){
		cassandraBase.getInstance().execute('update TX_TRANSACTIONS set status=2 where txid = ?',[uuid],null,this.dummyCallback,this);
	};
	
	this.rollbackTransaction=function(){
		cassandraBase.getInstance().execute('update TX_TRANSACTIONS set status=3 where txid = ?',[uuid],null,this.dummyCallback,this);
	};
	
	this.txCallback4CreateTransaction=function(rows,statement,thus){
		cassandraBase.getInstance().execute('insert into TX_TRANSACTIONS(txId,start_date,status) values(?,dateof(now()),?)',[rows[0].sessionid,1],null,thus.dummyCallback,thus);
		var txObject = new TxObject(rows[0].sessionid);
		txObject.txCallback=function(some){
			logger.info(this.getTransactionId() + " transaction is created.");
		};
		txObject.txCallback("sd");
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