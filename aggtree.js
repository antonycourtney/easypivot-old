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

      function applyPath( path ) {
        // TODO: Think about how to use rollupBy or a smaller number of groupBys that get chopped up 
        // and cache result for efficiency

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

        var pivotColumnInfo = { id: "_pivot", type: "text", displayName: " " };

        if( path.length < pivotColumns.length ) {
          pathQuery = pathQuery
                        .groupBy( [ pivotColumns[ path.length ] ], baseSchema.columns )
                        .mapColumnsByIndex( { 0 : pivotColumnInfo } ); 
        } else {
          // leaf level
          var outCols = [ "_pivot" ];
          outCols = outCols.concat( baseSchema.columns );
          pathQuery = pathQuery
                        .extend( [ "_pivot" ], { "_pivot": { type: "text", displayName: " " } } )
                        .project( outCols );
        }

        // TODO: Should we optionally also insert _childCount and _leafCount columns?
        // _childCount would count next level of groupBy, _leafCount would count pathQuery at point of filter.
        // Both of these are potentially quite expensive unless we are very careful in the implementation.
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