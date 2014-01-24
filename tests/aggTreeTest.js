

var rt = relTab.local();


var q0 = relTab.query.table( "bart-comp-all" ).project( [ "Job", "Title", "Union", "Name", "Base", "TCOE" ]);
var p0 = aggTree.vpivot( rt, q0, ["Job", "Title" ] );

console.log( "called vpivot, got promise result..." );

p0.then( function( tree0 ) {
  console.log( "vpivot initial promise resolved..." );

  var rq0 = tree0.rootQuery;
  runQueryTest( rq0, "root query" );

  var q1 = tree0.applyPath( [] );
  runQueryTest( q1, "applyPath( [] ) ", 3, function( res ) {

    var expCols = [ "_depth", "_pivot", "Job", "Title", "Union", "Name", "Base", "TCOE"];

    deepEqual( res.schema.columns, expCols, "Q1 schema columns" );
    deepEqual( res.rowData.length, 19, "Q1 rowData length" );

    var actSum = columnSum( res, "TCOE" );

    deepEqual( actSum, 349816190, "Q1 rowData sum(TCOE)" );  
  });

  var q2 = tree0.applyPath( [ "Executive Management" ] );
  runQueryTest( q2, "query for path /'Executive Management' " );

  var q3 = tree0.applyPath( [ "Executive Management", "General Manager" ] );
  runQueryTest( q3, "query for path /Executive Management/General Manager " );
  

  var openPaths = { "Executive Management": { "General Manager": true }, "Safety": true,  };

  var q4 = tree0.getTreeQuery( openPaths );
  runQueryTest( q4, "treeQuery" );

  // seems a bit odd that we have to call this explicitly, but...
  start();
} );
