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
      return updateTreeTable();
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
      return updateTreeTable();
    }

    this.closePath = function( path ) {
      removePath( path );
      return updateTreeTable();
    }


    function updateTreeTable() {
        /*
         * ???  Open Design Question: Should we cancel any pending operation here??
         *
         * One way to do so would be to get our hands on the Q.defer() object and call .reject().
         * But going to be a bit of work to set up the plumbing for that and that still doesn't address how 
         * we actually propagate a true cancellation through RelTab so that it can actually send a cancellation
         * if there is a remote server / thread doing the work.
         *
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

    /* get promise<query> for the current state of the pivot table */
    this.getCurrentImage = function() {
      return treeQueryPromise;
    }

    updateTreeTable();
  }

  function mkPivotTreeModel( rt, baseQuery, initialPivots ) {
    return new PivotTreeModel( rt, baseQuery, initialPivots );
  }

}(jQuery));