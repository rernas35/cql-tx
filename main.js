/**
 * New node file
 */
var restServer = require('./rest-interface');
var webSocketServer = require('./websocket-interface');
var checker = require('./session-checker');

restServer.initRestServer();
webSocketServer.initWebSocketServer();
checker.schedule();



