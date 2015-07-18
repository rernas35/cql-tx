/**
 * New node file
 */

//var NodeCache = require( "node-cache" );
//var myCache = new NodeCache( { stdTTL: 100, checkperiod: 120 } );
//
//obj = { my: "Special", variable: 42 };
//myCache.set( "myKey", obj, function( err, success ){
//  if( !err && success ){
//    console.log( success );
//    // true
//    // ... do something ...
//  }
//});
//
//myCache.get( "myKey", function( err, value ){
//	  if( !err ){
//	    console.log( value );
//	    // { "myKey": { my: "Special", variable: 42 } }
//	    // ... do something ...
//	  }
//	});


//var cql = require('node-cassandra-cql');
//var client = new cql.Client({hosts: ['localhost'], keyspace: 'mykeyspace'});
////client.execute('SELECT fname,lname FROM fusers WHERE fname=?', ['ilker'],
//client.execute('ALTER TABLE fusers add newc2 varchar', [],
//  function(err, result) {
//    if (err) console.log(err);
//    else console.log('got user profile with email ' + result);
//  }
//);


//var WebSocketServer = require('ws').Server
//  , wss = new WebSocketServer({port: 9980});
//wss.on('connection', function(ws) {
//    ws.on('message', function(message) {
//        console.log('received: %s', message);
//    });
//    ws.send('something');
//});