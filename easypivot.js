(function ($) {
  'use strict';
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
    var path = (pathStr.length > 0 ) ? pathStr.split( aggTree.PATHSEP ) : [];
    return path;
 }

  function SimpleDataView() {
    var rawData = [];
    var idMap = [];
    var sortCmpFn = null;

    function getLength() {
      return rawData.length;
    }

    function getItem(index) {
      return rawData[index];
    }

    function getItemById(id) {
      return idMap[id];
    }

    function setItems( items ) {
      rawData = items;
      idMap = items.slice();
      updateView();
    }

    function setSort( cmpFn ) {
      sortCmpFn = cmpFn;
      updateView();
    }

    function updateView() {
      if( sortCmpFn )
        sort( sortCmpFn );
    }

    function sort( cmpFn ) {
      rawData.sort( cmpFn );
    }

    return {
      "getLength": getLength,
      "getItem": getItem,
      "setItems": setItems,
      "setSort": setSort,
      "getItemById": getItemById,
    };
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
    var dataView = new SimpleDataView();

    this.rt = rt;
    this.dataView = dataView;

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

    this.loadDataView = function( tableData ) {
      // var ts = relTab.fmtTableData( tableData );
      // console.log( "loadDataView:\n", ts );

      var nPivots = pivots.length;
      var rowData = [];
      var parentIdStack = [];
      for( var i = 0; i < tableData.rowData.length; i++ ) {
        var rowMap = tableData.schema.rowMapFromRow( tableData.rowData[ i ] );
        var path = EasyPivot.parsePath( rowMap._path );
        var depth = rowMap._depth;
        rowMap.isOpen = this.pathIsOpen( path );
        rowMap.isLeaf = depth > nPivots;
        rowMap._id = i;
        parentIdStack[ depth ] = i;
        var parentId = ( depth > 0 ) ? parentIdStack[ depth - 1 ] : null;
        rowMap._parentId = parentId;
        rowData.push( rowMap );
      }

      dataView.setItems( rowData );
      dataView.schema = tableData.schema;

      return dataView;
    }

    /*
     * refresh the pivot tree based on current model state.
     * returns: promise<SimpleDataView> for that yields flattened, sorted tabular view of the pivot tree.
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

      var treeQueryPromise = vpivotPromise.then( function( ptree ) {
        return ptree.getTreeQuery( openNodeMap );
      });

      var m = this;
      var dvPromise = treeQueryPromise
                      .then( function( treeQuery ) { return rt.evalQuery( treeQuery ); } )
                      .then( function( tableData ) { return m.loadDataView( tableData ); } );

      return dvPromise;
    }

    // recursively get ancestor of specified row at the given depth:
    function getAncestor( row, depth ) {
      if( depth > row._depth ) {
        throw new Error( "getAncestor: depth " + depth + " > row.depth of " 
                         + row._depth + " at row " + row._id );
      }
      while ( depth < row._depth ) {
        row = dataView.getItemById( row._parentId );
      };
      return row;
    }

    this.setSort = function(column, dir) {
      var sortcol = column;
      var sortdir = dir;

      var orderFn = ( dir > 0 ) ? d3.ascending : d3.descending;

      function cmpFn( ra, rb ) {
        var idA = ra._id;
        var idB = rb._id;

        if( ra._depth==0 || rb._depth==0 )
          return (ra._depth - rb._depth); // 0 always wins

        if( ra._depth < rb._depth ) {
          // get ancestor of rb at depth ra._depth:
          rb = getAncestor( rb, ra._depth );
          if( rb._id == ra._id ) {
            // ra is itself an ancedstor of rb, so comes first:
            return -1;
          }
        } else if( ra._depth > rb._depth ) {
          ra = getAncestor( ra, rb._depth );
          if( ra._id == rb._id ) {
            // rb is itself an ancestor of ra, so must come first:
            return 1;
          }
        }

        var ret = orderFn( ra[ sortcol ], rb[ sortcol ]);
        return ret;
      }

      this.dataView.setSort( cmpFn );
    }
  }

  function mkPivotTreeModel( rt, baseQuery, initialPivots ) {
    return new PivotTreeModel( rt, baseQuery, initialPivots );
  }

}(jQuery));