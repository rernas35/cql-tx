/**
 * logger file
 */

var winston = require('winston');
var config = require("./config");
winston.emitErrs = true;

var logger = new winston.Logger({
    transports:config.winstonTransports(winston),
    exitOnError: false
});

var cqltxLogger =  { debug : function(txObject,str){
	if (txObject == undefined)
		logger.debug(str);
	else 
		logger.debug('[' + txObject.getTransactionId() + ']' + str);
	},
	info : function(txObject,str){
		if (txObject == undefined)
			logger.info(str);
		else 
			logger.info('[' + txObject.getTransactionId() + ']' + str);
	},
	error : function(txObject,str){
		if (txObject == undefined)
			logger.error(str);
		else 
			logger.error('[' + txObject.getTransactionId() + ']' + str);
	},
	warn : function(txObject,str){
		if (txObject == undefined)
			logger.warn(str);
		else 
			logger.warn('[' + txObject.getTransactionId() + ']' + str);
	}
};

module.exports = cqltxLogger;
