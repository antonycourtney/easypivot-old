(function ($) {
  // Slick.Grid
  $.extend(true, window, {
    CSVView: {
      View: CSVViewer,
      Data: {
        RemoteModel: RemoteModel 
      }
    }
  });

  /***
   * A sample AJAX data store implementation.
   * Modified by ANT to load CSV data from a Python script served via cherrypy
   */
  function RemoteModel( tn ) {
    // private
    var PAGESIZE = 50;
    var data = {length: 0};
    var searchstr = "";
    var sortcol = null;
    var sortdir = 1;
    var h_request = null;
    var req = null; // ajax request
    var table_name = tn;

    // events
    var onDataLoading = new Slick.Event();
    var onDataLoaded = new Slick.Event();


    function init() {
    }


    function isDataLoaded(from, to) {
      for (var i = from; i <= to; i++) {
        if (data[i] == undefined || data[i] == null) {
          return false;
        }
      }

      return true;
    }


    function clear() {
      for (var key in data) {
        delete data[key];
      }
      data.length = 0;
    }


    function ensureData(from, to) {
      // console.log( "ensureData( ", from, ", ", to, " ) ")
      if (req) {
        req.abort();
        for (var i = req.fromPage; i <= req.toPage; i++)
          data[i * PAGESIZE] = undefined;
      }

      if (from < 0) {
        from = 0;
      }

      if (data.length > 0) {
        to = Math.min(to, data.length - 1);
      }

      var fromPage = Math.floor(from / PAGESIZE);
      var toPage = Math.floor(to / PAGESIZE);

      while (data[fromPage * PAGESIZE] !== undefined && fromPage < toPage)
        fromPage++;

      while (data[toPage * PAGESIZE] !== undefined && fromPage < toPage)
        toPage--;

      // console.log( " fromPage: ", fromPage, ", toPage: ", toPage );

      if (fromPage > toPage || ((fromPage == toPage) && data[fromPage * PAGESIZE] !== undefined)) {
        // TODO:  look-ahead
        onDataLoaded.notify({from: from, to: to});
        return;
      }

      var url = "tables/" + tn
      if (h_request != null) {
        clearTimeout(h_request);
      }

      // console.log( "about to send request for [ ", from, ", ", to,  " ) ");

      h_request = setTimeout(function () {
        for (var i = fromPage; i <= toPage; i++)
          data[i * PAGESIZE] = null; // null indicates a 'requested but not available yet'

        onDataLoading.notify({from: from, to: to});

        reqParams = { startRow: fromPage * PAGESIZE, rowLimit: (((toPage - fromPage) * PAGESIZE) + PAGESIZE)};

        if (sortcol != null )
          reqParams.sortby = sortcol + ((sortdir > 0) ? "+asc" : "+desc");

        req = $.get( url, reqParams, onSuccess ); 

        req.fromPage = fromPage;
        req.toPage = toPage;
      }, 50);
    }


    function onError(fromPage, toPage) {
      alert("error loading pages " + fromPage + " to " + toPage);
    }

    function onSuccess(resp) {
      console.log( "Got ", resp.results.length, " rows ( of ", resp.totalRowCount, " ) from server, startRow: ", resp.request.startRow );
      // console.log( "onSuccess!" );
      // console.log( resp );
      
      var from = resp.request.startRow, to = from + resp.results.length;
      data.length = parseInt( resp.totalRowCount );  

      for (var i = 0; i < resp.results.length; i++) {
        var item = resp.results[i];

        data[from + i] = item;
        data[from + i].index = from + i;
      }

      req = null;

      onDataLoaded.notify({from: from, to: to});
    }


    function reloadData(from, to) {
      for (var i = from; i <= to; i++)
        delete data[i];

      ensureData(from, to);
    }


    function setSort(column, dir) {
      sortcol = column;
      sortdir = dir;
      clear();
    }

    init();

    return {
      // properties
      "data": data,

      // methods
      "clear": clear,
      "isDataLoaded": isDataLoaded,
      "ensureData": ensureData,
      "reloadData": reloadData,
      "setSort": setSort,

      // events
      "onDataLoading": onDataLoading,
      "onDataLoaded": onDataLoaded
    };
  }


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