test( "hello test", function() {
  ok( 1 == "1", "Passed!" );
  ok( true, "Second subtest");
});
test("basic reltab functionality", function() {
  var rf = RelTab.Filter;
  var e1 = rf.And().eq("x",25).eq("y","hello");
  var s1 = e1.toSqlWhere();
  console.log( s1 );
  ok( s1 == "x=25 and y='hello'", "basic filter expression: " + s1 );


  var e2 = rf.And()
            .eq("x",30).eq("y","goodbye")
            .subExp( rf.Or().gt("z",50).gt("a","b") );
  var s2 = e2.toSqlWhere();
  console.log( s2 );  
  ok( s2 == "x=30 and y='goodbye' and ( z>50 or a>'b' )", "filter with subexp and or: " + s2 );

  function onGetSchema( error, schema ) {
    console.log( "onGetSchema: ", error, schema );

    var expectedCols = ["Name", "Title", "Base", "OT", "Other", "MDV", "ER", 
                      "EE", "DC", "Misc", "TCOE", "Source", "Job", "Union"];

    columns = schema.columns; // array of strings
    console.log( "columns: ", columns );

    deepEqual( columns, expectedCols, "getSchema column ids" );


    var columnTypes = [];

    for ( var i = 0; i < columns.length; i++ ) {
      var colId = columns[ i ];
      var ct = schema.getColumnType( colId );
      console.log( "column '" + colId + "': type: " + ct );
      columnTypes.push( ct );
    }
    console.log( "columnTypes: ", columnTypes );

    var expectedColTypes = ["text", "text", "integer", "integer", "integer", 
      "integer", "integer", "integer", "integer", "integer", "integer", "text", "text", "text"];

    deepEqual( columnTypes, expectedColTypes, "getSchema column types" );

    start();
  }

  var q1 = RelTab.Query()
            .table( "bart-comp-all" );

  console.log( q1.toString() );
  ok( true, "basic query expression construction" );

  var rt = RelTab.Local();

  asyncTest( "async test: getSchema", 2, function() {
    rt.getSchema( q1, onGetSchema );
  });
  // TODO: Maybe query result should be some kind of generator or stream instead
  // of [[]]

  function onQueryResult( error, res ) {
    console.log( "onQueryResult: ", error, res );
    console.log( "result length: ", res.length );
    equal( res.length, 2873, "q1 results length");

    console.log( res[0]);

    var expRow0 = ["Crunican, Grace", "General Manager", 312461, 0, 3846, 19141, 37513,
      17500, 1869, 7591, 399921, "MNP", "Executive Management", "Non-Represented"];

    deepEqual( res[0], expRow0, "q1 row 0 " );

    start();
  }

  asyncTest( "asyncTest: evalQuery q1", 2, function() {
    rt.evalQuery( q1, onQueryResult );
  });

  var q2 = q1.project( [ "Name", "Title", "TCOE", "Job" ] );

  function onQ2Result( error, res ) {
    console.log( "onQ2Result: ", error, res );
    console.log( "result length: ", res.length );
    equal( res.length, 2873, "q2 results length");

    console.log( res[0]);

    var expRow0 = ["Crunican, Grace", "General Manager", 399921,  "Executive Management" ];

    deepEqual( res[0], expRow0, "q2 row 0 " );

    start();
  }

  asyncTest( "asyncTest: evalQuery q2", 2, function() {
    rt.evalQuery( q2, onQ2Result );
  });

/*
  var q2 = q1.select( rf.And().gt("TCOE",200000).eq("Job Family","Transportation Operations") )
             .project( [ "Name", "Title", "TCOE", "Job Family" ] );
  console.log( q2.toString() );
*/
});

test( "next test", function() {
 ok( true, "Another Test!");
});