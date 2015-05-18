/**
 * New node file
 */
var cassandraBase = require('./cassandra-base');

function Session(txId) {
	this.callback;
	this.tableRows = [];
	this.tableIndex = -1;
	this.txId = txId;
	this.tableName = '';
	this.changeMap = new Array();
	this.columnMap = new Array();
	this.changeCount = -1;
	
	this.wholeChangeCheck = 0;
	this.tableColumnCheck = 0;

	this.commitTransaction=function(){
		var boundCallback = this.retrieveTablesCallback.bind(this);
		cassandraBase.getInstance().execute1('select table_name from tx_tables',[],null,boundCallback);
//		cassandraBase.getInstance().execute('update TX_TRANSACTIONS set status=2 where txid = ?',[uuid],null,this.dummyCallback,this);
	};
	
	this.retrieveTablesCallback = function(rows,statement){
		this.tableRows = rows; 
		this.tableIndex = 0;
		for(var i=this.tableIndex;i<this.tableRows.length;i++){
			var item = this.tableRows[i];
			var boundCallback = this.addColumns.bind({root:this,tableName:item.table_name});
			cassandraBase.getInstance().execute1("select * from tx_columns where table_name=?",["'" + item.table_name + "'"],null,boundCallback);
		}
		
	};
	
	this.addColumns = function(rows,statement){
		var tableName = this.tableName;
		var columnList = new Array();
		for(var i=0;i<rows.length;i++){
			columnList.push(rows[i]);
		}
		this.root.columnMap[tableName] = columnList;
		this.root.tableColumnCheck++;
		if (this.root.tableColumnCheck == this.root.tableRows.length){
			this.root.tableColumnCheck=0;
			this.root.processData();
		}
			
	}
	
	this.processData=function(){
		this.tableIndex=0;
		this.changeCount = 0;
		for(var i=this.tableIndex;i<this.tableRows.length;i++){
			var boundCallback = this.processDataCallback.bind({root:this,tableName:this.tableRows[i].table_name});
			cassandraBase.getInstance().execute1("select * from tx_" + this.tableRows[i].table_name + " where trx_id=?",[this.txId],null,boundCallback);
		}
	}
	
	this.processDataCallback= function(rows,statement){
		var changeList = new Array();
		for(var i=0;i<rows.length;i++){
			changeList.push(rows[i]);
			this.root.changeCount++;
		}
		this.root.changeMap[this.tableName] = changeList;
		this.root.tableColumnCheck++;
		if (this.root.tableColumnCheck == this.root.tableRows.length){
			this.root.tableColumnCheck=0;
			this.root.processChange();
		}
	}
	
	
	this.processChange=function(){
		this.wholeChangeCheck = 0 ;
		for (var tIndex=0;tIndex<this.tableRows.length;tIndex++){
			var tableName = this.tableRows[tIndex].table_name; 
			var changeList = this.changeMap[tableName];
			for (var i=0;i<changeList.length;i++){
				var change = changeList[i];
				console.log('change.action : ' + this.prepareChangeStatement(change, tableName) );
				var boundCallback = this.removeChange.bind({root:this,tableName:tableName});
				cassandraBase.getInstance().execute1(this.prepareChangeStatement(change, tableName),[],null,boundCallback);
			} 
		}
	}
	
	this.removeChange = function(){
		this.root.wholeChangeCheck++;
		if (this.root.wholeChangeCheck == this.root.changeCount){
			var boundCallback = this.root.removeChangeCallback.bind({root:this.root});
			cassandraBase.getInstance().execute1('delete from tx_' + this.tableName + ' where trx_id = ?',[this.root.txId],null, boundCallback )
		}
		
	}
	
	this.removeChangeCallback = function(){
			this.root.callback();
	}
	
	this.prepareChangeStatement = function(change,tableName){
		var columnList = this.columnMap[tableName];
		if (change.action == 1){
			var columnStatement = '';
			var valueStatement = '';
			var index = 0;
			for (var i=0;i<columnList.length;i++){
				var col = columnList[i];
				if (index > 0){
					columnStatement += ",";
					valueStatement += ",";
				}
				columnStatement += col.column_name;
				var isText = (col.column_type == 'org.apache.cassandra.db.marshal.UTF8Type');
				if (isText){
					valueStatement += "'";
				}
				valueStatement += change[col.column_name];
				if (isText){
					valueStatement += "'";
				}
				index++;
			}
			return "insert into " + tableName + "(" + columnStatement + ") values(" + valueStatement + ")";
		}else if (change.action == 2){
			var setStatement = '';
			var whereStatement = '';
			var index = 0,wIndex=0;
			var changeSet = change.changeset.split(',');
			for (var i=0;i<columnList.length;i++){
				var col = columnList[i];
				if (col.key_type == 'partition_key' ){
					if (wIndex > 0){
						whereStatement += " and ";
					}
					whereStatement += col.column_name + '=';
					var isText = (col.column_type == 'org.apache.cassandra.db.marshal.UTF8Type');
					if (isText){
						whereStatement += "'";
					}
					whereStatement += change[col.column_name];
					if (isText){
						whereStatement += "'";
					}
					wIndex++;
				}else {
					if (changeSet.indexOf(col.column_name) == -1)
						continue;
					if (index > 0){
						setStatement += ",";
					}
					setStatement += col.column_name + '=';
					var isText = (col.column_type == 'org.apache.cassandra.db.marshal.UTF8Type');
					if (isText){
						setStatement += "'";
					}
					setStatement += change[col.column_name];
					if (isText){
						setStatement += "'";
					}
					index++;
				}
				
			}
			return "update " + tableName + " set " + setStatement + " where " + whereStatement ;
		}
		
		
	}
	
		
	
}




module.exports = {
		getInstance : function(txid){
			return new Session(txid);
		}	
}
