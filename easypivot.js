(function ($) {
  $.extend(true, window, {
    EasyPivot: {
      pivotTreeModel: mkPivotTreeModel
    }
  });


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

    this.setPivots = function( inPivots ) {
      pivots = inPivots;
      needPivot = true;
      updateTreeTable();
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

    function removePath() {
      // TODO:
    }


    this.openPath = function( path ) {
      addPath( path );
      updateTreeTable();
    }

    this.closePath = function( path ) {
      removePath( path );
      updateTreeTable();
    }

    /*
     * make a promise handler that will invoke the given listener function iff
     * it hasn't been removed from listener list.
     */
    function mkPromiseHandler( lid, lfn ) {
      function handler( treeQuery ) {
        if( listeners[ lid ] )
          return lfn( treeQuery );
        return null;
      }
      return handler;
    }


    this.addListener = function( lfn ) {
      /*
       * TODO: We need to call treeQueryPromise.then() and add ourselves to listener list.
       * But then they key is that whenever we re-assign treeQueryPromise (in updateTreeTable),
       * we need to call then() to register all listeners on the new promise.
       */

      // What we really want here is to somehow call then() on whatever treeQueryPromise is.
      // pre: treeQueryPromise !== null       
      var lid = listeners.length;
      listeners.push( lfn );
      treeQueryPromise.then( mkPromiseHandler( lid, lfn ) );
      return lid;
    }

    this.removeListener = function( lid ) {
      delete listeners[ lid ];
    }

    function updateTreeTable() {
      if( treeQueryPromise ) {
        // We reject any existing promise in case it is pending from some previous action.
        // Note that this will be a NOP if the promise is already resolved.
        /*
         * My basic plan here is to make PivotTreeModel as stateless and flexible as possible
         * and then implement some kind of blocking in the UI so that, for example, opening a node blocks until
         * that completes.  Probably need to expose some additional machinery for notifying the UI when some
         * action is pending or completed...
         */
        treeQueryPromise.reject( "pending action cancelled by additional update" );
      }

      if( needPivot ) {
        vpivotPromise = aggTree.vpivot( rt, baseQuery, pivots );

      };

      treeQueryPromise = ptp.then( function( ptree ) {
        return ptree.getTreeQuery( openNodeMap );
      });

      // and re-register all our listeners on new promise:
      for( var i = 0; i < listeners.length; i++ ) {
        var lfn = listeners[ i ];
        if( lfn === undefined ) continue;
        treeQueryPromise.then( mkPromiseHandler( lid, lfn ) );
      }
    }

    updateTreeTable();
  }

  function mkPivotTreeModel( rt, baseQuery, initialPivots ) {
    return new PivotTreeModel( rt, baseQuery, initialPivots );
  }

}(jQuery));