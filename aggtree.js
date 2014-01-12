(function ($) {
  $.extend( true, window, {
    aggTree: {
      vpivot: vpivotTree,
    }
  });

/*
  function AggTreeQueryExp( inChain, opRep ) {
    var _expChain = inChain.slice();  // shallow copy inChain
    if ( opRep )
      _expChain.push( opRep );

    function opExpToString() {
      var rs = this.operator + " ( " + this.args.toString() + " )";
      return rs;
    }

    // variadic function to make an operator applied to args node in AST
    function mkOperator(opName)
    {
      var args = Array.prototype.slice.call(arguments);
      args.shift();
      var opRep = { operator:opName, args:args, toString: opExpToString };
      var e = new AggTreeQueryExp( _expChain, opRep );
      return e;
    }

    function mkRelTabRef( relTabQuery ) {
      return mkOperator( "reltab", relTabQuery );
    }

    function mkVPivot( pivotColumns ) {
      return mkOperator( "vpivot", pivotColumns );
    }

    return {
      "reltab": mkRelTabRef,
      "vpivot": mkVPivot,
      "toString": toString,
      "_getRep": function() { return _expChain; }
    }
  }

  function createQueryExp( rtq ) {
    var base = new AggTreeQueryExp( [] );
    return base.reltab( rtq );
  }
*/

  function vpivotTree( rt, rtBaseQuery, pivotColumns ) {
/*
    // obtain schema for base query:
    // TODO:  Don't want to evaluate entire query just to get schema!  FIX!
    var baseSchema = rt.evalQuery( rtBaseQuery ).schema;

    // Prepend pivot column:
    // ??? Do we want to do the schema manipulation here at all?  Perhaps we should do it at
    // flattening level instead...
    // TODO: ensure pivot column doesn't already exist / generate a fresh name!
    var cmd = baseSchema.columnMetadata.slice();
    var cols = baseSchema.columns.slice();
    cmd["_pivot"] = { "type": "text", "displayName": " " };
    cols.unshift( "_pivot" );
    var resSchema = new rt.Schema( { "columns": cols, "columnMetadata": cmd } );
*/
    function applyPath( path ) {
      // TODO: Think about how to use rollupBy and cache result for efficiency

      // queries are immutable so no need to clone:
      var pathQuery = rtBaseQuery;

      // We will filter by all path components, and then group by the next pivot Column:
      if ( path.length > pivotColumns.length ) {
        throw new Error( "applyPath: path length > pivot columns" );
      }

      if ( path.length > 0 ) {
        var pred = rt.filter.and();
        for ( var i = 0; i < path.length; i++ ) {
          pred = pred.and( pivotColumns[i], path[i] );
        }
        pathQuery = pathQuery.filter( pred );
      }

      if( path.length < pivotColumns.length ) {
        pathQuery = pathQuery.groupBy( [ pivotColumns[ path.length ] ] );
      }

      return pathQuery;  
    }

    return { 
      // schema: resSchema,
      applyPath: applyPath 
    };
  }


  function localAggTree( relTabImpl ) {
    var rt = relTabImpl;

    function evalTreeExp( treeExp ) {

    }

    return {
      "evalTreeExp": evalTreeExp,
    }
  } 
}(jQuery));