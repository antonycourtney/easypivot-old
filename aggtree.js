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

      var outCols = [ "_depth", "_pivot" ];
      outCols = outCols.concat( baseSchema.columns );

      var rootQuery = rtBaseQuery
                    .groupBy( [], baseSchema.columns )
                    .extendColumn( "_pivot", { type: "text" }, null ) 
                    .extendColumn( "_depth", { type: "integer" }, 0 )
                    .project( outCols ); 

      /*
       * returns a query for the children of the specified path:
       */
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

        var pivotColumnInfo = { id: "_pivot", type: "text", displayName: "_pivot" };

        if( path.length < pivotColumns.length ) {
          pathQuery = pathQuery
                        .groupBy( [ pivotColumns[ path.length ] ], baseSchema.columns )
                        .mapColumnsByIndex( { 0 : pivotColumnInfo } )
        } else {
          // leaf level
          pathQuery = pathQuery
                        .extendColumn( "_pivot", { type: "text" }, null );
        }

        // add _depth column and project to get get column order correct:
        pathQuery = pathQuery
                      .extendColumn( "_depth", { type: "integer" }, path.length + 1 )
                      .project( outCols ); 


        // TODO: Should we optionally also insert _childCount and _leafCount ?
        // _childCount would count next level of groupBy, _leafCount would do count() at point of calculating
        // filter for current path (before doing groupBy).
        // These can certainly have non-trivial costs to calculate
        return pathQuery;
      }

      return { 
        // schema: resSchema,
        rootQuery: rootQuery,
        applyPath: applyPath 
      };
    }

    return basep.then( withBaseRes );
  }

}(jQuery));