(function ($) {
  $.extend( true, window, {
    aggTree: {
      vpivot: vpivotTree,
    }
  });



  function vpivotTree( rt, rtBaseQuery, pivotColumns ) {

    // obtain schema for base query:
    // TODO:  Don't want to evaluate entire query just to get schema!
    // Need to change interface of RelTab to return a true TableDataSource that has calculated
    // Schema but not yet calculated rowdata...
    var basep = rt.evalQuery( rtBaseQuery );

    function withBaseRes( baseRes ) {
      var baseSchema = baseRes.schema;

      /*
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
          var pred = relTab.and();
          for ( var i = 0; i < path.length; i++ ) {
            pred = pred.eq( pivotColumns[i], "'" + path[i] + "'" );
          }
          pathQuery = pathQuery.filter( pred );
        }

        if( path.length < pivotColumns.length ) {
          pathQuery = pathQuery.groupBy( [ pivotColumns[ path.length ] ], baseSchema.columns ); 
        }

        return pathQuery;  
      }

      return { 
        // schema: resSchema,
        applyPath: applyPath 
      };
    }

    return basep.then( withBaseRes );
  }

}(jQuery));