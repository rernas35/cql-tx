/**
 * New node file
 */

var config = {
		restPort : 8080,
		websocketPort : 8081,
		trxClearanceIntervalInMins : 10,
		cassandraClient : {contactPoints: ['127.0.0.1'], keyspace: 'mykeyspace'}
}


module.exports = config;