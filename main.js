/**
 * New node file
 */
var restServer = require('./rest-interface');
var webSocketServer = require('./websocket-interface');

restServer.initRestServer();
webSocketServer.initWebSocketServer();



