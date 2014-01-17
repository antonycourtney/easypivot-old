(function ($) {     
  // CSVView.View, CSVView.Data    
  $.extend(true, window, {
    CSVView: {
      View: CSVViewer,
      Data: {
        RemoteModel: RemoteModel,
        LocalModel: LocalModel 
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

    function loadInitialImage( cbfn ) {
      var url = "tables/" + tn;
      req = $.get( url, 
                   { startRow: 0, rowLimit: 50 },
                   cbfn );

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
      "loadInitialImage": loadInitialImage,

      // events
      "onDataLoading": onDataLoading,
      "onDataLoaded": onDataLoaded
    };
  }

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
   * A completely client-side data model based on d3's CSV parser.
   */
  function LocalModel( tn ) {
    // private
    var groupItemMetadataProvider = new Slick.Data.GroupItemMetadataProvider();    
    var dataView = new Slick.Data.DataView( { groupItemMetadataProvider: groupItemMetadataProvider } );
    var table_name = tn;

    // events
    var onDataLoading = new Slick.Event();
    var onDataLoaded = new Slick.Event();

    function setGrouping( header) {
        var aggs = [];

        for( i = 0; i < header.columns.length; i++ ) {
          var colId = header.columns[i];
          var cm = header.columnMetadata[ colId ];
          if( cm.type=="integer" || cm.type=="real" ) {
            aggs.push( new Slick.Data.Aggregators.Sum( colId ) );
          }
        }

        dataView.setGrouping([ {
        getter: "Union",
        formatter: function (g) {
          return "Union:  " + g.value + "  <span style='color:green'>(" + g.count + " items)</span>";
        },
        aggregators: aggs,
        aggregateCollapsed: true,
        collapsed: true
      }, {
        getter: "Job",
        formatter: function (g) {
          return "Job Family:  " + g.value + "  <span style='color:green'>(" + g.count + " items)</span>";
        },
        aggregators: aggs,
        aggregateCollapsed: true,
        collapsed: true
      } ]
      );
    }

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


    var numRE = /[-+]?[$]?[0-9,]*\.?[0-9]+([eE][-+]?[0-9]+)?/;

    function loadInitialImage( cbfn ) {
      var url = "json/" + tn + ".json";

      function onGet( error, data) {
        console.log( data );
        var header = data[ 0 ];
        var body = data[ 1 ];

        var columnIds = header.columns;

        rows = [];
        for( var i = 0; i < body.rowData.length; i++ ) {
          rowArray = body.rowData[ i ];
          rowDict = { 'id': "row_" + i }; 
          for( var col = 0; col < rowArray.length; col++ ) {
            rowDict[ columnIds[ col ] ] = rowArray[ col ];
          }
          rows[ i ] = rowDict;
        }

        firstRow = rows[0];
        console.log( firstRow );
        dataView.setItems( rows );
        setGrouping( header );
        response = { header: header, results: rows };
        cbfn( response );
      }

      d3.json(url, onGet);
    }

    /*
     * called after grid has been created to allow, e.g. registering
     * plugins
     */
    function onGridInit(grid) {
      // register the group item metadata provider to add expand/collapse group handlers
      grid.registerPlugin(groupItemMetadataProvider);
      // grid.setSelectionModel(new Slick.CellSelectionModel());   

      // wire up model events to drive the grid
      dataView.onRowCountChanged.subscribe(function (e, args) {
        grid.updateRowCount();
        grid.render();
      });

      dataView.onRowsChanged.subscribe(function (e, args) {
        grid.invalidateRows(args.rows);
        grid.render();
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
      "onGridInit": onGridInit,

      // events
      "onDataLoading": onDataLoading,
      "onDataLoaded": onDataLoaded
    };
  }


  function CSVViewer( container, loader )
  {
    var grid;

    var options = {
      editable: false,
      enableAddRow: false,
      enableCellNavigation: false
    };

    var loadingIndicator = null;

    function sumTotalsFormatter(totals, columnDef) {
      var val = totals.sum && totals.sum[columnDef.field];
      if (val != null) {
        return intFormatter( ((Math.round(parseFloat(val)*100)/100)) );
      }
      return "";
    }

    /* Create a grid from the specified set of columns */
    function createGrid( columns ) 
    {
      grid = new Slick.Grid(container, loader.data, columns, options);

      loader.onGridInit(grid);

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

    var intFormatter = d3.format( ",d" );

    function onInitialImage( response ) {
      // let's approximate the column width:
      var MINCOLWIDTH = 65;
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
      var hdr = response.header;
      var gridCols = [];
      for (var i = 0; i < hdr.columns.length; i++) {
        var colId = hdr.columns[ i ];
        var cmd = hdr.columnMetadata[ colId ];
        var ci = { id: colId, field: colId };
        var displayName = cmd.displayName || colId;
        ci.name = displayName;
        ci.toolTip = displayName;
        ci.sortable = true;
        ci.width = colWidths[ colId ];
        if( i==ci.length - 1 ) {
          // pad out last column to allow for dynamic scrollbar
          ci.width += GRIDWIDTHPAD;
        }
        if (ci.type=='integer') {
          ci.formatter = function ( r, c, v, cd, dc ) {
            return intFormatter( v );
          }
          ci.groupTotalsFormatter = sumTotalsFormatter;          
        } else if (ci.type=='real') {
          ci.groupTotalsFormatter = sumTotalsFormatter;
        }

        // console.log( "column ", i, " name: '", ci[i].name, "', width: ", ci[i].width );
        gridWidth += ci.width;
        gridCols.push( ci );
      }
      createGrid( gridCols );

      $(container).css( 'width', gridWidth+'px' );
    };

    loader.loadInitialImage( onInitialImage );
  };
}(jQuery));