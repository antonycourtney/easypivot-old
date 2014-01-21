(function ($) {
  $.extend(true, window, {
    PivotView: {
      View: PivotViewer,
      Data: {
        LocalModel: LocalModel 
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

  /***
   * Local data model that uses RelTab engine and base query (passed in) to 
   * construct an aggTree to query data
   */
  function LocalModel( rt, baseQuery, vpivots ) {
    // private
    var dataView = new SimpleDataView();



    // events
    var onDataLoading = new Slick.Event();
    var onDataLoaded = new Slick.Event();

    function init() {
    }

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

    function loadInitialImage( cbfn ) {

      function onGet( res ) {
        console.log( "loadInitialImage: ", res );

        // construct SlickGrid-style row data:
        var columnIds = res.schema.columns;

        rows = [];
        for( var i = 0; i < res.rowData.length; i++ ) {
          rowArray = res.rowData[ i ];
          rowDict = { };
          for( var col = 0; col < rowArray.length; col++ ) {
            rowDict[ columnIds[ col ] ] = rowArray[ col ];
          }
          rows.push( rowDict );
        }

        dataView.setItems( rows );


        // construct columnInfo:
        firstRow = rows[0];

        var names = Object.getOwnPropertyNames( firstRow );

        var gridCols = [];
        for (var i = 0; i < res.schema.columns.length; i++) {
          var colId = res.schema.columns[ i ];
          var cmd = res.schema.columnMetadata[ colId ];
          var ci = { id: colId, field: colId };
          var displayName = cmd.displayName || colId;
          ci.name = displayName;
          ci.toolTip = displayName;
          gridCols.push( ci );
        };
        response = { columnInfo: gridCols, results: rows };
        cbfn( response );
      }

      // var p = rt.evalQuery( baseQuery );
      var pTree = aggTree.vpivot( rt, baseQuery vpivots );
      pTree.then( function( tree ) {
        tree.applyPath( [ ] ).then( );
      });
    }


    function reloadData(from, to) {
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

    init();

    return {
      // properties
      "data": dataView,

      // methods
      "clear": clear,
      "isDataLoaded": isDataLoaded,
      "ensureData": ensureData,
      // "reloadData": reloadData,
      "setSort": setSort,
      "loadInitialImage": loadInitialImage,

      // events
      "onDataLoading": onDataLoading,
      "onDataLoaded": onDataLoaded
    };
  }


  function PivotViewer( container, loader )
  {
    var grid;

    var options = {
      editable: false,
      enableAddRow: false,
      enableCellNavigation: false
    };

    var loadingIndicator = null;

    /* Create a grid from the specified set of columns */
    function createGrid( columns ) 
    {
      grid = new Slick.Grid(container, loader.data, columns, options);

      grid.onViewportChanged.subscribe(function (e, args) {
        var vp = grid.getViewport();
        loader.ensureData(vp.top, vp.bottom);
      });

      grid.onSort.subscribe(function (e, args) {
        grid.setSortColumn( args.sortCol.field, args.sortAsc );
        loader.setSort(args.sortCol.field, args.sortAsc ? 1 : -1);
        var vp = grid.getViewport();
        loader.ensureData(vp.top, vp.bottom);
      });

      loader.onDataLoading.subscribe(function () {
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

      loader.onDataLoaded.subscribe(function (e, args) {
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

    function onInitialImage( response ) {
      // let's approximate the column width:
      var MINCOLWIDTH = 44;
      var MAXCOLWIDTH = 300;
      var GRIDWIDTHPAD = 16;
      var gridWidth = 0;  // initial padding amount
      var colWidths = {};
      var rowData = response.results;
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
      var ci = response.columnInfo;
      for (var i = 0; i < ci.length; i++) {
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
      createGrid( ci );

      $(container).css( 'width', gridWidth+'px' );
    };

    loader.loadInitialImage( onInitialImage );
  };
}(jQuery));