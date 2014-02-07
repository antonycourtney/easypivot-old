(function ($) {
  'use strict';
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

      var outCols = [ "_depth", "_pivot", "_path" ];
      outCols = outCols.concat( baseSchema.columns );

      var rootQuery = rtBaseQuery
                    .groupBy( [], baseSchema.columns )
                    .extendColumn( "_pivot", { type: "text" }, null ) 
                    .extendColumn( "_depth", { type: "integer" }, 0 )
                    .extendColumn( "_path", {type: "text"}, "" )
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

        // add _depth and _path column and project to get get column order correct:

        // TODO: Escape any embedded pipe chars in path!

        var basePathStr = "|" + path.join( "|" );
        var pathDelim = ( path.length > 0 ) ? "|" : "";

        pathQuery = pathQuery
                      .extendColumn( "_depth", { type: "integer" }, path.length + 1 )
                      // .extendColumn( "_path", { type: "text" }, pathStr )
                      .extendColumn( "_path", { type: "text" }, function( r ) { return basePathStr + pathDelim + r._pivot; } )
                      .project( outCols ); 


        // TODO: Should we optionally also insert _childCount and _leafCount ?
        // _childCount would count next level of groupBy, _leafCount would do count() at point of calculating
        // filter for current path (before doing groupBy).
        // These can certainly have non-trivial costs to calculate
        // Probably also want to explicitly insert _path0..._pathN columns!
        return pathQuery;
      }

      /*
       * get query for full tree state from a set of openPaths
       */
      function getTreeQuery( openPaths ) {
        var resQuery = this.rootQuery;
        
        function walkPath( pivotTree, treeQuery, prefix, pathMap ) {
          for( var component in pathMap ) {
            if( pathMap.hasOwnProperty( component ) ) {
              // add this component to our query:
              var subPath = prefix.slice();
              subPath.push( component );
              console.log( "walkPath: " + JSON.stringify( subPath ) );
              var subQuery = pivotTree.applyPath( subPath );
              treeQuery = treeQuery.concat( subQuery );

              // and recurse, if appropriate:
              var cval = pathMap[ component ];
              if( typeof cval == "object" ) {
                  treeQuery = walkPath( pivotTree, treeQuery, subPath, cval );
              }
            }
          }
          return treeQuery;
        }

        if( openPaths ) {
          resQuery = resQuery.concat( this.applyPath( [] ) );  // open root level!
        }
        var tq = walkPath( this, resQuery, [], openPaths );

        tq = tq.sort( [ [ "_path", true ] ] );
        return tq;
      }  

      return { 
        // schema: resSchema,
        rootQuery: rootQuery,
        applyPath: applyPath,
        getTreeQuery: getTreeQuery
      };
    }

    return basep.then( withBaseRes );
  }

}(jQuery));