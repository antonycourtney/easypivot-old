
var rt = relTab.local();

asyncTest("basic aggTree functionality", function() {
  var q0 = relTab.query().table( "bart-comp-all" ).project( [ "Job", "Title", "Union", "Name", "Base", "TCOE" ]);

  var p1 = aggTree.vpivot( rt, q0, ["Job", "Title" ] );

  console.log( "called vpiot, got promise result..." );

  p1.then( function( tree0 ) {
    console.log( "vpivot initial promise resolved..." );

    var q1 = tree0.applyPath( [] );
    console.log( "query for level 0 of tree: ", q1.toString() );
    console.log( q1 );

    var p2 = rt.evalQuery( q1 );
    p2.then( onQ1Result );
  } );

} );

function onQ1Result( res ) {
  console.log( "OnQ1Result: ", res );
  ok();
  start();
};