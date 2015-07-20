/**
 * New node file
 */
var txHandler = require('./transaction-handler');
var cassandraBase = require('./cassandra-base');
var config = require('./config');
var logger = require('./logger');
var moment = require('moment');

var txInstance = txHandler.getInstance();
var formatt = 'YYYY-MM-DD HH:mm:ssZZ';

function clearOpenSessions(){
	logger.info(undefined,'session clean started');
	cassandraBase.getInstance().execute1("select * from tx_transactions",[],null,clearOpenSessionsCallback);
}

function clearOpenSessionsCallback(rows){
	for(var i=0 ; i<rows.length ; i++){
		var trx = rows[i];
		var checkDate= moment().subtract(0,'minutes');
//		var checkDate= moment().subtract(config.trxClearanceIntervalInMins,'minutes');
//		var checkDateStr = checkDate.format(formatt)
		var trx_start_date = moment(new Date(trx.start_date));
		if (trx_start_date < checkDate){
			txInstance.rollbackTransaction(trx.txid, function() {
				
			},function(err) {
				
			});
		}
	}
}

function updateCallback(){
	logger.info("Session check finished.");
}
setInterval(clearOpenSessions,(5000));

module.exports = {
		schedule : function(){
//			setInterval(clearOpenSessions,(50000));
			setInterval(clearOpenSessions,(config.trxClearanceIntervalInMins * 60 * 1000));
		}	
}
