/**
 * New node file
 */

var ddlHandler = require('./cassandra-ddl-handler');
var dmlHandler = require('./cassandra-dml-handler');
var txHandler = require('./transaction-handler');

//CassandraDDLHandler().process4Metadata('insert into T1(sudur,budur) values(12,23)');

//ddlHandler.getInstance().process4Metadata('update users set fname=12 where lname=23');

//dmlHandler.getInstance().executeTransactional("insert into users(user_id,fname,lname) values(72,'342','234')",'uuid()');

var txInstance = txHandler.getInstance()
//txInstance.openTransaction(function(txid){console.log('open transaction callback %s' , this.sessionId)});
//txInstance.execute("insert into users(user_id,fname,lname) values(109,'ilker3 riza','ernas')","decd86eb-6f55-4f81-a8ee-ea771993ce79",function(){console.log('executed')});
//txInstance.execute("update users set lname='mahran' where user_id=106","dd9ea51f-6d99-44f9-bdd1-d87818be6113");
txInstance.commitTransaction('decd86eb-6f55-4f81-a8ee-ea771993ce79', function(){console.log('callllback')});




