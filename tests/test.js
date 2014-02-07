
asyncTest( "asyncTest: fetchURL tests", 1, function() {
  var goodUrl = "../json/bart-comp-all.json";

  var promise = relTab.fetchURL( goodUrl );

  promise.then( function( resp ) {
      console.log( "promise success! Response: ", resp );
      ok( resp.length == 2, "fetch good URL promise success" );
      start();
    }, function( error ) {
      console.log( "promise failed.  Error: ", error );
      start();
    } );
} );

asyncTest( "asyncTest: fetchURL with bad URL", function() {
  var badUrl = "bad.json";

  // See http://stackoverflow.com/questions/17544965/unhandled-rejection-reasons-should-be-empty
  // Q.stopUnhandledRejectionTracking();  // Q does something very odd that requires this..


  console.log( "about to call fetchURL" );
  var promise2 = relTab.fetchURL( badUrl );
  console.log( "fetchURL called, about to call promise.then..." );

  promise2.then( function( resp ) {
    console.log( "FAIL: unexpected success fetching bad URL" );
    start();
  }, function( reason ) {
    console.log( "badURL: promise failed, reason: ", reason );
    ok( true, "promise failed as expected for bad URL");
    start();
  } );
  console.log( "promise.then call complete, returning control");
});

test("basic reltab functionality", function() {
  var e1 = relTab.and().eq("x",25).eq("y","'hello'");
  var s1 = e1.toSqlWhere();
  console.log( s1 );
  ok( s1 == "x=25 and y='hello'", "basic filter expression: " + s1 );


  var e2 = relTab.and()
            .eq("x",30).eq("y","'goodbye'")
            .subExp( relTab.or().gt("z",50).gt("a","b") );
  var s2 = e2.toSqlWhere();
  console.log( s2 );  
  ok( s2 == "x=30 and y='goodbye' and ( z>50 or a>b )", "filter with subexp and or: " + s2 );
} );

var tcoeSum = 0;

function onQ1Result( res ) {
  console.log( "onQ1Result: ", res );

  var schema = res.schema;

  var expectedCols = ["Name", "Title", "Base", "OT", "Other", "MDV", "ER", 
                    "EE", "DC", "Misc", "TCOE", "Source", "Job", "Union"];

  columns = schema.columns; // array of strings
  console.log( "columns: ", columns );

  deepEqual( columns, expectedCols, "getSchema column ids" );

  var columnTypes = [];

  for ( var i = 0; i < columns.length; i++ ) {
    var colId = columns[ i ];
    var ct = schema.columnType( colId );
    // console.log( "column '" + colId + "': type: " + ct );
    columnTypes.push( ct );
  }
  console.log( "columnTypes: ", columnTypes );

  var expectedColTypes = ["text", "text", "integer", "integer", "integer", 
    "integer", "integer", "integer", "integer", "integer", "integer", "text", "text", "text"];

  deepEqual( columnTypes, expectedColTypes, "getSchema column types" );

  var rowData = res.rowData;

  console.log( "rowData: ", rowData );
  console.log( "rowData.length: ", rowData.length );
  equal( rowData.length, 2873, "q1 rowData.length");

  console.log( rowData[0]);

  var expRow0 = ["Crunican, Grace", "General Manager", 312461, 0, 3846, 19141, 37513,
    17500, 1869, 7591, 399921, "MNP", "Executive Management", "Non-Represented"];

  deepEqual( rowData[0], expRow0, "q1 row 0 " );

  tcoeSum = columnSum( res, "TCOE" );
  console.log( "tcoeSum: ", tcoeSum );

  start();
}

var q1 = relTab.query.table( "bart-comp-all" );

var rt = relTab.local();

asyncTest( "async test: simple table query", 5, function() {
  console.log( q1.toString() );
  ok( true, "basic query expression construction" );
  var p1 = rt.evalQuery( q1 );
  p1.then( onQ1Result ).fail( mkAsyncErrHandler( "simple table query" ) );
});

var q2 = q1.project( [ "Job", "Title", "Union", "Name", "Base", "TCOE" ] );

console.log( "q2: ", q2.toString() );

function onQ2Result( res ) {
  console.log( "onQ2Result: ", res );
  console.log( "onQ2Result schema: ", res.schema );
  console.log( "result length: ", res.rowData.length );
  equal( res.rowData.length, 2873, "q2 results length");

  console.log( res.rowData[0]);

  var expRow0 = ["Executive Management", "General Manager", "Non-Represented", "Crunican, Grace", 312461, 399921];

  deepEqual( res.rowData[0], expRow0, "q2 row 0 " );

  start();
}

asyncTest( "asyncTest: evalQuery q2", 2, function() {
  var p = rt.evalQuery( q2 );

  p.then( onQ2Result ).fail( mkAsyncErrHandler( "evalQuery q2" ) );
});

var q3 = q1.groupBy( [ "Job", "Title" ], [ "TCOE" ] );  // note: [ "TCOE" ] equivalent to [ [ "sum", "TCOE" ] ]

function onQ3Result( res ) {
  console.log( "onQ3Result", res );

  var rs = res.schema;
  var expCols = [ "Job", "Title", "TCOE" ];
  deepEqual( rs.columns, expCols, "q3 schema" );

  deepEqual( res.rowData.length, 380, "number of grouped rows in q3 result" );

  var groupSum = columnSum( res, "TCOE" );

  deepEqual( groupSum, tcoeSum, "tcoe sum after groupBy" );

  start();
}

asyncTest( "asyncTest: evalQuery q3", 3, function() {
  var p = rt.evalQuery( q3 );

  p.then( onQ3Result ).fail( mkAsyncErrHandler( "evalQuery q3" ) );
} );

var q4 = q2.groupBy( ["Job"], [ "Title", "Union", "Name", "Base", "TCOE" ] );

asyncTest( "asyncTest: evalQuery q4", 3, function() {
  var p = rt.evalQuery( q4 );
  p.then( onQ4Result ).fail( mkAsyncErrHandler( "evalQuery q4" ) );
})

function onQ4Result( res ) {
  console.log( "onQ4result", res );

  var rs = res.schema;

  var expCols = [ "Job", "Title", "Union", "Name", "Base", "TCOE" ];
  deepEqual( rs.columns, expCols );

  deepEqual( res.rowData.length, 19, "number of grouped rows in q4 result" );

  var groupSum = columnSum( res, "TCOE" );
  deepEqual( groupSum, tcoeSum, "tcoe sum after groupBy" );

  start();
}

var q5 = q1.filter( relTab.and().eq("Job", "'Executive Management'") );
runQueryTest( q5, "basic filter", 1, function( res ) {
  ok( res.rowData.length == 14, "expected row count after filter");
} );

var q6 = q1.mapColumns( { Name: { id: "EmpName", displayName: "Employee Name" } } );
runQueryTest( q6, "mapColumns" );

var q7 = q1.mapColumnsByIndex( { 0: { id: "EmpName" } } );
runQueryTest( q7, "mapColumnsByIndex" );

var q8 = q5.concat( q1.filter( relTab.and().eq("Job", "'Safety'") ) );
runQueryTest( q8, "concat" );

var q9 = q8.sort( [ [ "Name", true ] ] );
runQueryTest( q9, "sort" );

var q10 = q8.sort( [ [ "Job", true ], [ "TCOE", false ] ] );
runQueryTest( q10, "multi key sort" );

var ptree = relTab.parse( "function (r) { return r > 99; } " );
console.log( "result of relTab.parse: ", ptree );
