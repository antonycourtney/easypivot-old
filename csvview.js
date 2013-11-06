(function ($) {
  // Slick.Grid
  $.extend(true, window, {
    CSVView: {
      View: CSVViewer
    }
  });

  function CSVViewer( container, tableName )
  {
    var grid;
    var loader = new CSVView.Data.RemoteModel( tableName );

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

    var url = "tables/" + tableName;
    req = $.get( url, 
                { startRow: 0, rowLimit: 50 },
                onInitialImage );

    return req; // placeholder
  };
}(jQuery));