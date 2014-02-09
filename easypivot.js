(function ($) {
  $.extend(true, window, {
    EasyPivot: {
      pivotTreeModel: mkPivotTreeModel,
      parsePath: parsePath,
    }
  });

  /***
   * split a pipe-delimited path string into its components
   */
 function parsePath( pathStr ) {
    pathStr = pathStr.slice(1);
    var path = (pathStr.length > 0 ) ? pathStr.split('|') : [];
    return path;
 }

  /*
   * Implementation choice / questions:
   *   Should the output of the model be a query or TableData?
   *   Answer:  A query!  The consumer can always apply the query to get TableData, but they can't go the other
   *   direction.  Also enables the potential for additional composition of the resulting query...
   */
  function PivotTreeModel( rt, baseQuery, pivots ) {
    var openNodeMap = null;
    var listeners = [];
    var treeQueryPromise = null;
    var vpivotPromise = null;
    var needPivot = true; // pivots have been set, need to call vpivot()

    this.rt = rt;

    this.setPivots = function( inPivots ) {
      pivots = inPivots;
      needPivot = true;
      return this.refresh();
    }

    this.getPivots = function() { 
      return pivots;
    }

    function addPath( path ) {
      if( !openNodeMap )
        openNodeMap = {};

      var nm = openNodeMap;
      for( var i = 0; i < path.length; i++ ) {
        var subMap = nm[ path[ i ] ];
        if( !subMap ) {
          subMap = {};
          nm[ path[ i ] ] = subMap;
        }
        nm = subMap;
      }
    }

    function removePath( nodeMap, path ) {
      var entry = path.shift();
      if( path.length== 0 ) {
        delete nodeMap[ entry ];
      } else {
        var subMap = nodeMap[ entry ];
        if( subMap )
          removePath( subMap, path );
      }
    }

    this.openPath = function( path ) {
      addPath( path );
    }

    this.closePath = function( path ) {
      removePath( openNodeMap, path );
    }

    // TODO: Subtle sync issue here! Note that calls to pathIsOpen between
    // calling openPath and refresh() will return a value inconsisent with
    // the current state of the UI.
    this.pathIsOpen = function( path ) {
      if( !openNodeMap )
        return false;

      var nm = openNodeMap;
      for( var i = 0; i < path.length; i++ ) {
        var subMap = nm[ path[ i ] ];
        if( !subMap ) 
          return false;
        nm = subMap;
      }
      return true;
    }

    /*
     * refresh the pivot tree based on current model state.
     * returns: promise<query> for query that yields flattened view of the pivot tree.
     */
    this.refresh = function() {
        /*
         * Open Design Question: Should we cancel any pending operation here??
         *
         * One way to do so would be to get our hands on the Q.defer() object and call .reject().
         * But going to be a bit of work to set up the plumbing for that and that still doesn't address how 
         * we actually propagate a true cancellation through RelTab so that it can actually send a cancellation
         * if there is a remote server / thread doing the work.
         */      

      if( needPivot ) {
        vpivotPromise = aggTree.vpivot( rt, baseQuery, pivots );
        needPivot = false;
      };

      treeQueryPromise = vpivotPromise.then( function( ptree ) {
        return ptree.getTreeQuery( openNodeMap );
      });

      return treeQueryPromise;
    }

  }

  function mkPivotTreeModel( rt, baseQuery, initialPivots ) {
    return new PivotTreeModel( rt, baseQuery, initialPivots );
  }

}(jQuery));