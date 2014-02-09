// Create a promise error handler that will call start() to fail an asyncTest
function mkAsyncErrHandler( msg ) {
  function handler( err ) {
    console.error( msg + ": unexpected promise failure.  Error: %O ", err );
    start();
  }
  return handler;
}

function columnSum( tableData, columnId ) {
  var sum = 0;

  var colIndex = tableData.schema.columnIndex( columnId );
  for ( var i=0; i < tableData.rowData.length; i++ ) {
    sum += tableData.rowData[i][colIndex];
  };
  return sum;
}


function runQueryTest( q, nm, assertCount, cfn ) {
  function onQueryTestResult( res ) {
    console.log( "queryTest " + nm + " result: ", res);
    var ts = relTab.fmtTableData( res );
    console.log( ts );

    if( typeof cfn != "undefined" ) {
      cfn( res );
    }

    console.warn( "onQueryTestResult " + nm + ": calling start");
    start();
  }

  function testFn() {
    console.log( "queryTest " + nm + ": ", q.toString() );
    console.log( q );
    var p = rt.evalQuery( q );
    p.then( onQueryTestResult ).fail( mkAsyncErrHandler( "runQueryTest " + nm ) );
  }

  console.log( "runQueryTest " + name + ": calling asyncTest");
  if( typeof assertCount != undefined ) {
    asyncTest( nm, assertCount, testFn );
  } else {
    asyncTest( nm, testFn ); 
  }

  console.log( "runQueryTest " + name + ": asyncTest called.");

}
