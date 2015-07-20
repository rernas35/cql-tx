/**
 * New node file
 */

var cassandraBase = require('./cassandra-base');
var config = require('./config');
var logger = require('./logger');
var moment = require('moment');


var checkDate= moment().subtract(config.trxClearanceIntervalInMins,'minutes');
var checkDateStr = checkDate.format('YYYY-MM-DD HH:mm:ssZZ')
var formatt = 'YYYY-MM-DD HH:mm:ssZZ';

function clearOpenSessions(){
	logger.info(undefined,'session clean started');
//	var checkDate= moment().subtract(0,'minutes');
	var checkDate= moment().subtract(config.trxClearanceIntervalInMins,'minutes');
	var checkDateStr = checkDate.format(formatt)
	cassandraBase.getInstance().execute1("select * from tx_transactions where start_date <'?' and status=1 ALLOW FILTERING",[checkDateStr],null,clearOpenSessionsCallback);
}

function clearOpenSessionsCallback(rows){
	for(var i=0 ; i<rows.length ; i++){
		var trx = rows[i];
		var nn = new Date(trx.start_date);
		cassandraBase.getInstance().execute1("update tx_transactions set status = 4 where txid = ? and start_date = '?'",[trx.txid,nn.getTime()],null,updateCallback);
		
	}
}

function updateCallback(){
	logger.info('interval finished.');
	
}

module.exports = {
		schedule : function(){
			setInterval(clearOpenSessions,(config.trxClearanceIntervalInMins * 60 * 1000));
		}	
}
