/**
 * New node file
 */

var config = {
		restPort : 8080,
		websocketPort : 8081,
		trxClearanceIntervalInMins : 10,
		cassandraClient : {contactPoints: ['127.0.0.1'], keyspace: 'mykeyspace'},
		timezone : '+0300',
		winstonTransports : function(winston){
							return [
		                      new winston.transports.File({
		                          level: 'debug',
		                          filename: '/data/cqltx/logs/all-logs.log',
		                          handleExceptions: true,
		                          json: true,
		                          maxsize: 5242880, //5MB
		                          maxFiles: 5,
		                          colorize: false
		                      }),
		                      new winston.transports.Console({
		                          level: 'debug',
		                          handleExceptions: true,
		                          json: false,
		                          colorize: true
		                      })
		                  ];
		}
}


module.exports = config;