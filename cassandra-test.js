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
//txInstance.openTransaction();
//txInstance.execute("insert into users(user_id,fname,lname) values(77,'343','233')","dd9ea51f-6d99-44f9-bdd1-d87818be6002");
//txInstance.execute("update users set fname='bisi' where lname='falan' and user_id=1744","dd9ea51f-6d99-44f9-bdd1-d87818be6987");
txInstance.commitTransaction('dd9ea51f-6d99-44f9-bdd1-d87818be6002');

