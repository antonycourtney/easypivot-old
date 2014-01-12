
var rt = relTab.local();

asyncTest("basic aggTree functionality", function() {
  var q0 = relTab.query().table( "bart-comp-all" ).project( [ "Job", "Title", "Union", "Name", "Base", "TCOE" ]);

  var tree0 = aggTree.vpivot( rt, q0, ["Job", "Title" ] );

  console.log( "created basic pivot tree" );

  var q1 = tree0.applyPath( [] );

  console.log( "query for level 0 of tree: ", q1 );

  var p1 = rt.evalQuery( q1 );
  p1.then( onQ1Result );

} );

function onQ1Result( res ) {
  console.log( "OnQ1Result: ", res );
};