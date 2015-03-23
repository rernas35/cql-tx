/**
 * New node file
 */
var sqlStrUtilityy = require('./sql-str-utility');
console.log("############Update1########################"); 
var update1 = sqlStrUtilityy.SqlStrUtility()
		.getUpdateStatement("update  tablename set column1='value1',column2=value2 where criteria1=cvalue1");
console.log('table : ' +  update1.table);
update1.columns.forEach(function(entry) {
	console.log(entry);
});
update1.criterias.forEach(function(entry) {
	console.log(entry);
});

console.log("############Update1########################");
console.log("############Update2########################");
var update2 = sqlStrUtilityy.SqlStrUtility()
		.getUpdateStatement("UpdAte  tablename2    set   column3='value3' , column4=value4  where   criteria2=cvalue2");
update2.columns.forEach(function(entry) {
	console.log(entry);
});
update2.criterias.forEach(function(entry) {
	console.log(entry);
});
console.log("############Update2########################");
console.log("############Update3########################");
var parameters = ['\'1111\'','3333'];
var update3 = sqlStrUtilityy.SqlStrUtility()
		.getUpdateStatement("UpdAte  tablename2    set   column1=? , column2='String ki ne string'  where   criteria2=?",parameters);
update3.columns.forEach(function(entry) {
console.log(entry);
});
update3.criterias.forEach(function(entry) {
	console.log(entry);
});
console.log("############Update3########################");
console.log("############Insert1########################");
var insert = sqlStrUtilityy.SqlStrUtility()
		.getInsertStatement("insert into tablename(column1, column2) values(value1,'value2')");
console.log('table : ' +  insert.table);
insert.columns.forEach(function(entry) {
	console.log(entry);
});
console.log("############Insert1########################");


console.log("############Insert2########################");
var parameters4Insert = [1111];
var insert = sqlStrUtilityy.SqlStrUtility()
		.getInsertStatement("insert into tablename(column1, column2) values(?,'value2')",parameters4Insert);

insert.columns.forEach(function(entry) {
	console.log(entry);
});
console.log("############Insert2########################");

console.log('############isUpdate#######################');

var retIsUpdate = sqlStrUtilityy.SqlStrUtility().isUpdate('update daybil set falan=\'ewe\' where filan=\'sdsd\'');
console.log('retIsUpdate: ' + retIsUpdate)
console.log('############isUpdate#######################');


console.log('############isInsert#######################');

var retIsInsert = sqlStrUtilityy.SqlStrUtility().isUpdate('update daybil set falan=\'ewe\' where filan=\'sdsd\'');
console.log('retIsInsert: ' + retIsInsert)
console.log('############isInsert#######################');

