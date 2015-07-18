/**
 * New node file
 */

var txHandler = require('./transaction-handler');
var restify = require('restify');
var config = require("./config");


function respondWithoutTrx(req, res, next) {
	console.log(req.body.commandType);
	var cmd = req.body;
	var txInstance = txHandler.getInstance()
	if (cmd.commandType == 'openTransaction') {
		txInstance.openTransaction(function() {
			var response = initializeSuccesfulResponse();
			response.txId = this.getTransactionId() + '';
			console.log('this.sessionId : %s', this.sessionId);
			res.send(JSON.stringify(response));
			console.log('resp : %s', JSON.stringify(response));
			next();
		},
		function(err) {
			var response = initializeErrorResponse(err, 1);
			res.send(JSON.stringify(response));
			next();
		});
	} else if (cmd.commandType == 'commitTransaction') {
		txInstance.commitTransaction(cmd.txId, function() {
			var response = initializeSuccesfulResponse();
			res.send(JSON.stringify(response));
			next();
		},function(err) {
			var response = initializeErrorResponse(err, 1);
			res.send(JSON.stringify(response));
			next();
		});
	} else if (cmd.commandType == 'rollbackTransaction') {
		txInstance.commitTransaction(cmd.txId, function() {
			var response = initializeSuccesfulResponse();
			res.send(JSON.stringify(response));
			next();
		},function(err) {
			var response = initializeErrorResponse(err, 1);
			res.send(JSON.stringify(response));
			next();
		});
	} else if (cmd.commandType == 'execute') {
		txInstance.execute(cmd.cql, cmd.txId, function() {
			var response = initializeSuccesfulResponse();
			res.send(JSON.stringify(response));
			next();
		},function(err) {
			var response = initializeErrorResponse(err, 1);
			res.send(JSON.stringify(response));
			next();
		});
	}
}

function initRestServer(){
	var server = restify.createServer();

	server.use(restify.acceptParser(server.acceptable));
	server.use(restify.bodyParser());
	server.use(restify.queryParser());

	server.get('/cqltx', respondWithoutTrx);
	server.post('/cqltx', respondWithoutTrx);

	server.listen(config.restPort, function() {
		console.log('%s listening at %s', server.name, server.url);
	});
}


function initializeSuccesfulResponse(){
	return {status:0,responseCode:0,description:"OK"};
}

function initializeErrorResponse(err,code){
	return {status:1,responseCode:code,description:err.message,stack:err.stack};
}


module.exports = {
		initRestServer : initRestServer
}