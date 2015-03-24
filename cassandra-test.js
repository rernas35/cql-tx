/**
 * New node file
 */

var ddlHandler = require('./cassandra-ddl-handler');
var dmlHandler = require('./cassandra-dml-handler');

//CassandraDDLHandler().process4Metadata('insert into T1(sudur,budur) values(12,23)');

//ddlHandler.getInstance().process4Metadata('update users set fname=12 where lname=23');

dmlHandler.getInstance().executeTransactional("insert into users(user_id,fname,lname) values(72,'342','234')",'uuid()');

