
## Usage

install  cql-tx module 

npm install cql-tx

Go to cql-tx folder under node_modules folder 
update config.js file for logging path and cassandra configurations.
```javascript
var config = {
		restPort : 8080,
		websocketPort : 8081,
		trxClearanceIntervalInMins : 10,
		keyspace : 'mykeyspace',
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
```

- For transaction tables , execute init.js ;
node init.js

For testing create the following user table ; 
create table users(fname text,lname text,user_id bigint,primary key (user_id));

```bashscript
open a transaction by the following curl command ;
curl -H "Content-Type: application/json" -X POST -d '{"commandType" : "openTransaction" }' http://localhost:8080/cqltx
```
exeute the cql (change the transaction id with the value that's returned by open-transaction);
```bashscript
curl -H "Content-Type: application/json" -X POST -d '{"commandType" : "execute", "txId" : "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxx" ,"cql":"insert into users(user_id) values(64222)" }' http://localhost:8080/cqltx
```

check the user table ,  if the "64222" entry exists or not.

commit the transaction ;
```bashscript
curl -H "Content-Type: application/json" -X POST -d '{"commandType" : "commitTransaction", "txId" : "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxx" }' http://localhost:8080/cqltx
```

or rollback;
```bashscript
curl -H "Content-Type: application/json" -X POST -d '{"commandType" : "rollbackTransaction", "txId" : "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxx" }' http://localhost:8080/cqltx
```

### Tools

Created with [Nodeclipse](https://github.com/Nodeclipse/nodeclipse-1)
 ([Eclipse Marketplace](http://marketplace.eclipse.org/content/nodeclipse), [site](http://www.nodeclipse.org))   

Nodeclipse is free open-source project that grows with your contributions.
