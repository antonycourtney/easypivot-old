
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
  Q.stopUnhandledRejectionTracking();  // Q does something very odd that requires this..


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
  var rf = relTab.filter;
  var e1 = rf.and().eq("x",25).eq("y","hello");
  var s1 = e1.toSqlWhere();
  console.log( s1 );
  ok( s1 == "x=25 and y='hello'", "basic filter expression: " + s1 );


  var e2 = rf.and()
            .eq("x",30).eq("y","goodbye")
            .subExp( rf.or().gt("z",50).gt("a","b") );
  var s2 = e2.toSqlWhere();
  console.log( s2 );  
  ok( s2 == "x=30 and y='goodbye' and ( z>50 or a>'b' )", "filter with subexp and or: " + s2 );
} );

// Create a promise error handler that will call start() to fail an asyncTest
function mkAsyncErrHandler( msg ) {
  function handler( err ) {
    console.log( msg + ": unexpected promise failure.  Error: %O ", err );
    start();
  }
  return handler;
}

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
    var ct = schema.getColumnType( colId );
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

  start();
}

var q1 = relTab.query()
          .table( "bart-comp-all" );

var rt = relTab.local();

asyncTest( "async test: simple table query", 5, function() {
  console.log( q1.toString() );
  ok( true, "basic query expression construction" );
  var p1 = rt.evalQuery( q1 );
  p1.then( onQ1Result, mkAsyncErrHandler( "simple table query" ) );
});

var q2 = q1.project( [ "Name", "Title", "TCOE", "Job" ] );

function onQ2Result( res ) {
  console.log( "onQ2Result: ", res );
  console.log( "onQ2Result schema: ", res.schema );
  console.log( "result length: ", res.rowData.length );
  equal( res.rowData.length, 2873, "q2 results length");

  console.log( res.rowData[0]);

  var expRow0 = ["Crunican, Grace", "General Manager", 399921,  "Executive Management" ];

  deepEqual( res.rowData[0], expRow0, "q2 row 0 " );

  start();
}

asyncTest( "asyncTest: evalQuery q2", 2, function() {
  var p = rt.evalQuery( q2 );

  p.then( onQ2Result );
});

var q3 = q1.groupBy( [ "Job", "Title" ], [ "TCOE" ] );  // note: [ "TCOE" ] equivalent to [ [ "sum", "TCOE" ] ]

function onQ3Result( res ) {
  console.log( "onQ3Result", res );

  var rs = res.schema;
  var expCols = [ "Job", "Title", "TCOE" ];
  deepEqual( rs.columns, expCols, "q3 schema" );

  // TODO: check res.rowData
  ok( true, "onQ3result called" );

  start();
}

asyncTest( "asyncTest: evalQuery q3", 2, function() {
  var p = rt.evalQuery( q3 );

  p.then( onQ3Result );
} );
