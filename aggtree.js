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

        var pivotColumnInfo = { id: "_pivot", type: "text" };

        var outCols = [ "_depth", "_pivot" ];
        outCols = outCols.concat( baseSchema.columns );

        if( path.length < pivotColumns.length ) {
          pathQuery = pathQuery
                        .groupBy( [ pivotColumns[ path.length ] ], baseSchema.columns )
                        .mapColumnsByIndex( { 0 : pivotColumnInfo } )
                        .extendColumn( "_depth", { type: "integer" }, path.length )
                        .project( outCols ); 
        } else {
          // leaf level
          pathQuery = pathQuery
                        .extendColumn( "_pivot", { type: "text" }, null );
        }

        // add _depth column and project to get get column order correct:
        pathQuery = pathQuery
                      .extendColumn( "_depth", { type: "integer" }, path.length )
                      .project( outCols ); 


        // TODO: Should we optionally also insert _childCount, _leafCount and _depth columns?
        // _depth is trivial.
        // _childCount would count next level of groupBy, _leafCount would count pathQuery at point of filter.
        // These latter two are potentially quite expensive; need to be careful in the implementation.
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