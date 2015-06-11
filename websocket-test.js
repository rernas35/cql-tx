/**
 * New node file
 */
var WebSocket = require('ws')
  , ws = new WebSocket('ws://localhost:8080');
ws.on('open', function() {
//    ws.send('{"commandType" : "openTransaction" }');
    ws.send('{"commandType" : "commitTransaction","txId":"fe7b3b85-dbe5-4ffc-bdd6-8301c39d442b" }');
//	ws.send('{"commandType":"execute","txId":"fe7b3b85-dbe5-4ffc-bdd6-8301c39d442b","cql":"insert into users(user_id,fname,lname) values(106,\'abdul fattah\',\'anyname\')"}');
});
ws.on('message', function(message) {
    console.log('received: %s', message);
});