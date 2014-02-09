// functions and objects for connection EasyPivot model to SlickGrid
(function ($) {
  'use strict';
  $.extend(true, window, {
    EasyPivot: {
      SlickGrid: {
        sgView: mkSGView,
        sgController: mkSGController
      }
    }
  });

  function SimpleDataView() {
    var rawData = [];

    function getLength() {
      return rawData.length;
    }

    function getItem(index) {
      return rawData[index];
    }

    function setItems( items ) {
      rawData = items;
    }

    function sort( cmpFn ) {
      rawData.sort( cmpFn );
    }

    return {
      "getLength": getLength,
      "getItem": getItem,
      "setItems": setItems,
      "sort": sort,
    };
  }

  function SGView( container, ptmodel ) {     
    // private
    var dataView = new SimpleDataView();

    // events
    var onDataLoading = new Slick.Event();
    var onDataLoaded = new Slick.Event();


    var grid;

    var options = {
      editable: false,
      enableAddRow: false,
      enableCellNavigation: false
    };

    var loadingIndicator = null;

    function isDataLoaded(from, to) {
      return true;  // TODO: should return false before receiving initial image...
    }

    function clear() {
      dataView.setItems([]);
    }

    function ensureData(from, to) {
      // TODO: Should probably check for initial image not yet loaded
      // onDataLoading.notify({from: from, to: to});
      onDataLoaded.notify({from: from, to: to});
    }

    function setSort(column, dir) {
      sortcol = column;
      sortdir = dir;

      var orderFn = ( dir > 0 ) ? d3.ascending : d3.descending;
   
      function cmpFn( ra, rb ) {
        return orderFn( ra[ sortcol ], rb[ sortcol ]);
      }

      dataView.sort( cmpFn );
      /* onDataLoaded.notify({from: 0, to: 50});  */
    }

    function onGridClick( e, args ) {
      console.log( "onGridClick: ", e, args );
      var item = this.getDataItem( args.row );
      console.log( "data item: ", item );
      if( item.isLeaf )
        return;
      var path = EasyPivot.parsePath( item._path );
      if( item.isOpen ) {
        ptmodel.closePath( path );
      } else {
        ptmodel.openPath( path);
      }

      refreshFromModel();
    }


    /* Create a grid from the specified set of columns */
    function createGrid( columns, data ) 
    {
      grid = new Slick.Grid(container, data, columns, options);

      grid.onViewportChanged.subscribe(function (e, args) {
        var vp = grid.getViewport();
        ensureData(vp.top, vp.bottom);
      });

/*
 * TODO...kind of difficult!
 *
      grid.onSort.subscribe(function (e, args) {
        grid.setSortColumn( args.sortCol.field, args.sortAsc );
        setSort(args.sortCol.field, args.sortAsc ? 1 : -1);
        var vp = grid.getViewport();
        ensureData(vp.top, vp.bottom);
      });
*/
      grid.onClick.subscribe( onGridClick );


      onDataLoading.subscribe(function () {
        if (!loadingIndicator) {
          loadingIndicator = $("<span class='loading-indicator'><label>Buffering...</label></span>").appendTo(document.body);
          var $g = $(container);

          loadingIndicator
              .css("position", "absolute")
              .css("top", $g.position().top + $g.height() / 2 - loadingIndicator.height() / 2)
              .css("left", $g.position().left + $g.width() / 2 - loadingIndicator.width() / 2);
        }

        loadingIndicator.show();
      });

      onDataLoaded.subscribe(function (e, args) {
        for (var i = args.from; i <= args.to; i++) {
          grid.invalidateRow(i);
        }

        grid.updateRowCount();
        grid.render();

        if (loadingIndicator)
          loadingIndicator.fadeOut();
      });

      /*$(window).resize(function () {
          grid.resizeCanvas();
      });
      */

      // load the first page
      grid.onViewportChanged.notify();
    };

    // 
    var _defaults = {
      groupCssClass: "slick-group",
      groupTitleCssClass: "slick-group-title",
      totalsCssClass: "slick-group-totals",
      groupFocusable: true,
      totalsFocusable: false,
      toggleCssClass: "slick-group-toggle",
      toggleExpandedCssClass: "expanded",
      toggleCollapsedCssClass: "collapsed",
      enableExpandCollapse: true,
      groupFormatter: defaultGroupCellFormatter,
    };

    // options = $.extend(true, {}, _defaults, options);
    options = _defaults; // for now

    function defaultGroupCellFormatter(row, cell, value, columnDef, item) {
      if (!options.enableExpandCollapse) {
        return item._pivot;
      }

      var indentation = item._depth * 15 + "px";

      var pivotStr = item._pivot || "";

      var ret = "<span class='" + options.toggleCssClass + " " +
          ((!item.isLeaf ) ? ( item.isOpen ? options.toggleExpandedCssClass : options.toggleCollapsedCssClass ) : "" ) +
          "' style='margin-left:" + indentation +"'>" +
          "</span>" +
          "<span class='" + options.groupTitleCssClass + "' level='" + item._depth + "'>" +
            pivotStr +
          "</span>";
      return ret;
    }

    /*
     * load tableData from ptModel in to DataView
     */
    function loadDataView( tableData ) {

      var nPivots = ptmodel.getPivots().length;
      var rowData = [];
      for( var i = 0; i < tableData.rowData.length; i++ ) {
        var rowMap = tableData.schema.rowMapFromRow( tableData.rowData[ i ] );
        var path = EasyPivot.parsePath( rowMap._path );
        rowMap.isOpen = ptmodel.pathIsOpen( path );
        rowMap.isLeaf = rowMap._depth > nPivots;
        rowData.push( rowMap );
      }

      dataView.setItems( rowData );

      return rowData;
    }

    function loadDataAndRender( tableData ) {
      loadDataView( tableData );

      var ts = relTab.fmtTableData( tableData );
      console.log( ts );

      grid.invalidateAllRows(); // TODO: optimize
      grid.updateRowCount();
      grid.render();      
    };

    function refreshFromModel() {
      ptmodel.refresh()
              .then( function( treeQuery ) { return rt.evalQuery( treeQuery) } )
              .then( loadDataAndRender );
    }


    function onInitialImage( tableData ) {
      console.log( "loadInitialImage: ", tableData );

      // construct SlickGrid-style row data:
      var columnIds = tableData.schema.columns;


      var rowData = loadDataView( tableData );

      // construct columnInfo:
      var firstRow = rowData[0];

      var names = Object.getOwnPropertyNames( firstRow );

      var gridCols = [];
      for (var i = 0; i < tableData.schema.columns.length; i++) {
        var colId = tableData.schema.columns[ i ];
       /* if ( colId[0] == "_" ) {
          if( colId !== "_pivot")
            continue;
        }
        */
        var cmd = tableData.schema.columnMetadata[ colId ];
        var ci = { id: colId, field: colId };
        var displayName = cmd.displayName || colId;
        ci.name = displayName;
        ci.toolTip = displayName;
        gridCols.push( ci );
      };

      // let's approximate the column width:
      var MINCOLWIDTH = 80;
      var MAXCOLWIDTH = 300;
      var GRIDWIDTHPAD = 16;
      var gridWidth = 0;  // initial padding amount
      var colWidths = {};
      for ( var i = 0; i < rowData.length; i++ ) {
        var row = rowData[ i ];
        var cnm;
        for ( cnm in row ) {
          var cellVal = row[ cnm ];
          if( cellVal ) {
            colWidths[ cnm ] = Math.min( MAXCOLWIDTH,
                Math.max( colWidths[ cnm ] || MINCOLWIDTH, 8 + ( 6 * cellVal.toString().length ) ) );
          } else {
            colWidths[ cnm ] = MINCOLWIDTH;
          }
        }
      }
      var ci = gridCols;
      for (var i = 0; i < ci.length; i++) {
        if( ci[i].id === "_pivot" ) {
          ci[i].name = "";
          ci[i].formatter = options.groupFormatter;
        }
        ci[i].toolTip = ci[i].name;
        ci[i].sortable = true;
        ci[i].width = colWidths[ ci[i].field ];
        if( i==ci.length - 1 ) {
          // pad out last column to allow for dynamic scrollbar
          ci[i].width += GRIDWIDTHPAD;
        }
        // console.log( "column ", i, " name: '", ci[i].name, "', width: ", ci[i].width );
        gridWidth += ci[i].width;
      }
      createGrid( ci, dataView );

      $(container).css( 'width', gridWidth+'px' );
    };
 
    var rt = ptmodel.rt;

    var pData = ptmodel.refresh()
                .then( function( treeQuery ) { return rt.evalQuery( treeQuery) } )
                .then( onInitialImage );
  }


  function mkSGView( div, ptmodel ) {
    return new SGView( div, ptmodel );
  }

  function SGController( sgview, ptmodel ) {
    // TODO
  }

  function mkSGController( sgview, ptmodel ) {
    return new SGController( sgview, ptmodel );
  }


}(jQuery));