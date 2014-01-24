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

/*
 * format table as a string for debugging:
 */
function fmtTableData( tbl, opts ) {
  var maxRows = ( opts && opts.maxRows ) || 50;
  var cellWidth = 16;
  var outStrs = [];

  function arrayInit( val, count ) {
    var ret = [];
    for ( var i = 0; i < count; i++ ) {
      ret.push( val );
    }
    return ret;
  }

  function strRepeat( s, count ) {
    var strs = arrayInit( s, count );
    return strs.join("");
  }

  var numFmt = d3.format( ",.2f" );

  function cellFmt( v ) {
    var s;
    if( v == null ) {
      s = ""
    } else {
      s = v.toString();
    }

    if( s.length >= cellWidth ) {
      s = s.substr( 0, cellWidth - 3 ) + "...";
    }
    // wish we had a repeat...
    var pre = strRepeat( " ", cellWidth - s.length );
    return pre + s;
  }

  var sCols = tbl.schema.columns;
  var colNames = sCols.map( function(colId) { return tbl.schema.displayName( colId ); } );
  var colHdrs = colNames.map( cellFmt );
  var ss = "| " + colHdrs.join(" | ") + " |";
  outStrs.push( ss );

  var dashCell = strRepeat( "-", cellWidth );
  var dashStrs = arrayInit( dashCell, sCols.length );
  var dashLine = "|-" + dashStrs.join("-|-") + "-|";

  outStrs.push( dashLine );

  var nRows = d3.min( [ maxRows, tbl.rowData.length ] );

  for( i = 0 ; i < nRows; i++ ) {
    var row = tbl.rowData[ i ];
    var cellStrs = row.map( cellFmt );
    outStrs.push( "| " + cellStrs.join( " | " ) + " |" );
  }

  if( nRows < tbl.rowData.length ) {
    outStrs.push( " ...output truncated ( " + ( tbl.rowData.length - nRows ) + " additional rows )" );
  }

  return outStrs.join( "\n" );
}


function runQueryTest( q, nm, assertCount, cfn ) {
  function onQueryTestResult( res ) {
    console.log( "queryTest " + nm + " result: ", res);
    var ts = fmtTableData( res );
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
