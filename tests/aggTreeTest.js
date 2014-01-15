
function columnSum( tableData, columnId ) {
  var sum = 0;

  var colIndex = tableData.schema.columnIndex( columnId );
  for ( var i=0; i < tableData.rowData.length; i++ ) {
    sum += tableData.rowData[i][colIndex];
  };
  return sum;
}
var rt = relTab.local();

asyncTest("basic aggTree functionality", 3, function() {
  var q0 = relTab.query().table( "bart-comp-all" ).project( [ "Job", "Title", "Union", "Name", "Base", "TCOE" ]);

  var p0 = aggTree.vpivot( rt, q0, ["Job", "Title" ] );

  console.log( "called vpiot, got promise result..." );

  p0.then( function( tree0 ) {
    console.log( "vpivot initial promise resolved..." );

    var q1 = tree0.applyPath( [] );
    console.log( "query for level 0 of tree: ", q1.toString() );
    console.log( q1 );

    var p1 = rt.evalQuery( q1 );
    p1.then( onQ1Result );

    var q2 = tree0.applyPath( [ "Executive Management" ] );
    console.log( "query for path /Executive Management: ", q2.toString() );
    console.log( q2 );

    var p2 = rt.evalQuery( q2 );
    p2.then( onQ2Result );

    // TODO: call start after all promises fulfilled.

  } );


} );

function onQ1Result( res ) {
  console.log( "OnQ1Result: ", res );
  console.log( "schema cols: ", res.schema.columns );
  console.log( "rowData length: ", res.rowData.length );

  var expCols = ["Job", "Job", "Title", "Union", "Name", "Base", "TCOE"];

  deepEqual( res.schema.columns, expCols, "Q1 schema columns" );
  deepEqual( res.rowData.length, 19, "Q1 rowData length" );

  var actSum = columnSum( res, "TCOE" );

  deepEqual( actSum, 349816190, "Q1 rowData sum(TCOE)" );
};

function onQ2Result( res ) {
  console.log( "onQ2Result:", res );

  start();
}