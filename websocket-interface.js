/**
 * New node file
 */

var txHandler = require('./transaction-handler');
var ws = require('ws');
// 	{"commandType" : "openTransaction" }  --> {"status": 0 ,"responseCode": 0,"description" : "OK,go on" , txId : "uuid" }
//	{"commandType" : "commitTransaction", "txId" : "uuid" }  --> {"status": 0 ,"responseCode": 0, "description" : "OK,go on"}
//	{"commandType" : "rollbackTransaction", "txId" : "uuid" }  --> {"status": 0 ,"responseCode": 0, "description" : "OK,go on"}
//	{"commandType" : "execute", "txId" : "uuid" }  --> {"status": 0 ,"responseCode": 0, "description" : "OK,go on"}
var WebSocketServer = ws.Server
  , wss = new WebSocketServer({port: 8080});
wss.on('connection', function(ws) {
    ws.on('message', function(message) {
        console.log('server received: %s', message);
        cmd = JSON.parse(message);
        var txInstance = txHandler.getInstance()
        if (cmd.commandType == 'openTransaction'){
        	txInstance.openTransaction(function(){
        		var response = initializeSuccesfulResponse();
        		response.txId = this.sessionId + '';
        		console.log('this.sessionId : %s',this.sessionId);
        		ws.send(JSON.stringify(response));
        	});
        }else if (cmd.commandType == 'commitTransaction'){
        	txInstance.commitTransaction(cmd.txId,function(){
        		var response = initializeSuccesfulResponse();
        		ws.send(JSON.stringify(response));
        	});
        }else if (cmd.commandType == 'rollbackTransaction'){
        	txInstance.commitTransaction(cmd.txId,function(){
        		var response = initializeSuccesfulResponse();
        		ws.send(JSON.stringify(response));
        	});
        }else if (cmd.commandType == 'execute'){
        	txInstance.execute(cmd.cql,cmd.txId,function(){
        		var response = initializeSuccesfulResponse();
        		ws.send(JSON.stringify(response));
        	});
        }
        console.log('bune : %s',cmd.bune);
    });
   
});

function initializeSuccesfulResponse(){
	return {status:0,responseCode:0,description:"OK"};
}
