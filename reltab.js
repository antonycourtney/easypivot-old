(function ($) {
  $.extend(true,window, {
    relTab: {
      local: localRelTab,
      remote: relTabClient,
      query: createQueryExp,
      filter: {
        and: createFilterAndExp,
        or: createFilterOrExp
      },
      fetchURL: fetchURL
    } 
  });

  /**
   * Wrapper around d3.json using Q's Promises library:
   */
  function fetchURL( url ) {
    var deferred = Q.defer();

    function onLoad( err, data ) {
      if( err )
        deferred.reject( err );

      deferred.resolve( data );
    }

    d3.json( url, onLoad );

    return deferred.promise;
  }


  /**
   * Enable construction of a RelTab Filter expression by operator chaining.
   * 
   * Example:
   * ```js
   *   var fgen = EasyPivot.RelTab.FilterExp.And;
   *
   *   var exp0 = fgen().gt("TCOE",400000).eq("Job Family","Transportation Operations");
   * ```
   */

   /* Constructor function */
  function RelTabFilterExp(boolOp) {
    var _exp = {
      boolOp: boolOp,
      args: []  // array of conjunctions / disjuncts
    };

    function mkColumnRelOp( relOp, colName, cmpVal ) {
      return {
        type: "columnRelOp",
        relOp: relOp,
        column: colName,
        value: cmpVal
      };
    }

    function ppVal(v) {
      var isString = (typeof v == "string" );
      var s = "";

      if (isString) {
        // FIXME / TODO: escape any quotes in string!
        s = "'" + v  + "'";
      } else {
        s = v.toString();
      }
      return s;
    }

    var ppOpMap = {
      "eq": "=",
      "gt": ">",
      "ge": ">=",
      "lt": "<",
      "le": "<="
    };

    function ppColumnRelOp(exp) {
      var s = exp.column + ppOpMap[exp.relOp] + ppVal( exp.value );
      return s;
    }

    function ppSubExp(subExp) {
      var s = "( " + subExp.exp.toSqlWhere() + " )";
      return s;
    }

    var ppFuncs = {
      columnRelOp: ppColumnRelOp,
      subexp: ppSubExp
    };

    /**
     * pretty print the expression as a SQL where clause
     */
    function ppExp(exp) {
      var strs = [];
      for( i = 0; i < exp.args.length; i++ ) {
        subExp = exp.args[i];
        var ppFunc = ppFuncs[ subExp.type ];
        strs.push( ppFunc( subExp ) );
      }
      var rs = strs.join(" " + exp.boolOp + " ");
      return rs;
    }

    function toSqlWhere() {
      return ppExp(_exp);
    }

    function mkEq(colName,cmpVal) {
      _exp.args.push( mkColumnRelOp( "eq", colName, cmpVal ) );
      return this;
    }

    function mkGt(colName,cmpVal) {
      _exp.args.push( mkColumnRelOp( "gt", colName, cmpVal ) );
      return this;
    }

    function mkSubExp( subExp ) {
      _exp.args.push( { type: "subexp", exp: subExp } );
      return this;
    }
    return {
      "eq": mkEq,
      "gt": mkGt,
      "subExp": mkSubExp,
      "toSqlWhere": toSqlWhere,
      "toString": function() {
        var rs = "[filter-exp (" + this.toSqlWhere() + ") ]";
        return rs;
      }
    };
  }  

  function createFilterAndExp() {
    return new RelTabFilterExp("and");
  }

  function createFilterOrExp() {
    return new RelTabFilterExp("or");
  }

  /*
   * Returns an (initially empty) RelTab Query Spec that
   * allows a query specification to be constructed using
   * operator chaining.
   */

  // constructor
  function RelTabQueryExp(inChain, opRep) {
    var _expChain = inChain.slice(); // shallow copy inChain
    if( opRep )
      _expChain.push( opRep );

    function opExpToString() {
      var rs = this.operator + " ( " + this.args.toString() + " )";
      return rs;
    }

    // variadic function to make an operator applied to args node in AST
    function mkOperator(opName)
    {
      var args = Array.prototype.slice.call(arguments);
      args.shift();
      var opRep = { operator:opName, args:args, toString: opExpToString };
      var e = new RelTabQueryExp( _expChain, opRep );
      return e;
    }

    function mkTableRef( tableName ) {
      return mkOperator( "table", tableName );
    }

    function mkSelectExp( fexp ) {
      return mkOperator( "select", fexp );
    }

    function mkProjectExp( cols ) {
      return mkOperator( "project", cols );
    }

    function mkGroupBy( cols, aggs ) {
      return mkOperator( "groupBy", cols, aggs );
    }

    function toString() {
      var es = [];
      for( var i = 0; i < _expChain.length; i++ ) {
        var exp = _expChain[i];
        es.push( exp.toString() );
      }
      return es.join(" >>> ");
    }

    function getRep() {
      return _expChain;
    }

    return {
      "table": mkTableRef,
      "select": mkSelectExp,
      "project": mkProjectExp,
      "toString": toString,
      "groupBy": mkGroupBy,
      // "rowCount"
      // "join"
      // "sort"
      // "extend": mkExtendExp,
      // "crossTab"
      "_getRep": getRep,
    };
  }

  function createQueryExp() {
    return new RelTabQueryExp([]);
  }


  /*
   * Pure client-side implementation of RelTab engine on RelTab JSON data files
   */
  function localRelTab() {
    var tableCache = {};

    function Schema( schemaData ) {
      var s = schemaData;

      s.getColumnType = function( colId ) {
        var md = s.columnMetadata[ colId ];

        return md.type;
      }

      var columnIndices = {};
      for ( var i = 0; i < schemaData.columns.length; i++ ) {
        var col = schemaData.columns[ i ];
        columnIndices[ col ] = i;
      }

      s.columnIndices = columnIndices;

      s.columnIndex = function( colId ) {
        return this.columnIndices[ colId ];
      }

      return s;  // for now
    }

    function TableRep( tableData ) {
      var schema = new Schema( tableData[ 0 ] );
      var rowData = tableData[ 1 ].rowData;

      function getRow( i ) {
        return rowData[ i ];
      }

      return {
        "schema": schema,
        "getRow": getRow,
        "rowData": rowData
      }
    }

    function ensureLoaded( tableName, cbfn ) {
      // tableCache will map tableName to a Promise whose value is a TableRep

      var tcp = tableCache[ tableName ];
      if( !tcp ) {
        var url = "../json/" + tableName + ".json";        

        console.log( "ensureLoaded: table '", tableName, "' not in cache, loading URL." );

        // We'll use .then to construct a Promise that gives us a TableRep (not just the raw JSON) and store this
        // in the tableCache:

        var rawJsonPromise = fetchURL( url );

        tcp = rawJsonPromise.then( function( jsonData ) {
          var trep = new TableRep( jsonData );
          return trep;
        })

        tableCache[ tableName ] = tcp;
      } else {
        console.log( "ensureLoaded: table '", tableName, "': cache hit!" );
      }
      return tcp;
    }

    function tableRefImpl( tableName ) {
      return ensureLoaded( tableName );
    }

    // Given an input Schema and an array of columns to project, calculate permutation
    // to apply to each row to obtain the projection
    function calcProjectionPermutation( inSchema, projectCols ) {
      var perm = [];
      // ensure all columns in projectCols in schema:
      for ( var i = 0 ; i < projectCols.length; i++ ) {
        var colId = projectCols[ i ];
        if( !( inSchema.columnMetadata[ colId ] ) ) {
          err = new Error( "project: unknown column Id '" + colId + "'" );
          throw err;
        }
        perm.push( inSchema.columnIndex( colId ) );
      }
      return perm;
    }

    function projectImpl( projectCols ) {

      // Singleton promise to calculate permutation and schema
      var psp = null;

      /* Use the inImpl schema and projectCols to calculate the permutation to
       * apply to each input row to produce the result of the project.
       */
      function calcState( inSchema ) {
        console.log( "calcState" );

        var perm = calcProjectionPermutation( inSchema, projectCols );
        var ns = Object.create( inSchema );
        ns.columns = projectCols;

        return { "schema": ns, "permutation": perm };
      }

      function pf( tableData ) {
        var ps = calcState( tableData.schema );
        function permuteOneRow( row ) {
          return d3.permute( row, ps.permutation);
        }
        var outRowData = tableData.rowData.map( permuteOneRow );

        return { "schema": ps.schema, "rowData": outRowData };
      }

      return pf;
    };


    function filterImpl( fexp ) {
      function ff( tableData ) {
        return tableData; // TODO!
      }
    };

    // A simple op is a function from a full evaluated query result { schema, rowData } -> { schema, rowData }
    // This can easily be wrapped to make it async / promise-based / caching
    function groupByImpl( cols, aggs ) {

      function calcSchema( inSchema ) {
        var gbs = Object.create( inSchema );
        gbs.columns = cols.concat( aggs ); // TODO: deal with explicitly specified (non-default) aggregations!

        return gbs;
      }

      function fillArray(value, len) {
        var arr = [];
        for (var i = 0; i < len; i++) {
          arr.push(value);
        };
        return arr;
      }

      function gb( tableData ) {

        console.log( "gb: enter ");

        var inSchema = tableData.schema;
        var outSchema = calcSchema( inSchema );

        var aggCols = aggs; // TODO: deal with explicitly specified (non-default) aggregations!

        var groupMap = {};
        var keyPerm = calcProjectionPermutation( inSchema, cols );
        var aggColsPerm = calcProjectionPermutation( inSchema, aggCols );

        // TODO: deal with non-sum aggs:
        var aggZeros = fillArray( 0, aggs.length );
        var aggFuncs = fillArray( function (x, y) { return x+y; }, aggs.length );

        for ( var i = 0; i < tableData.rowData.length; i++ ) {
          var inRow = tableData.rowData[ i ];

          var keyData = d3.permute( inRow, keyPerm );
          var aggInData = d3.permute( inRow, aggColsPerm );
          var keyStr = JSON.stringify( keyData );

          var groupRow = groupMap[ keyStr ];
          var aggOutData = undefined;
          if ( !groupRow ) {
            aggOutData = aggZeros.slice();
          } else {
            aggOutData = groupRow.slice( keyData.length, keyData.length + aggs.length );
          }
          for ( var j = 0; j < aggOutData.length; j++ ) {
            var af = aggFuncs[ j ];
            aggOutData[ j ] = af( aggOutData[ j ], aggInData[ j ] );
          }
          // and put it back in our map:
          var outRow = keyData.concat( aggOutData );
          groupMap[ keyStr ] = outRow;
        }  
        rowData = [];
        for ( keyStr in groupMap ) {
          if ( groupMap.hasOwnProperty( keyStr ) ) {
            outRow = groupMap[ keyStr ];
            rowData.push( outRow );
          }
        }

        console.log( "gb: exit" );
        return { schema: outSchema, rowData: rowData };
      }

      return gb;
    }

    var simpleOpImplMap = {
      "filter": filterImpl,
      "project": projectImpl,
      "groupBy": groupByImpl
    }

    // given a chain of simple operators (TableData -> TableData functions), compose them in to a
    // TableData -> TableData function 
    function evalSimpleExpChain( expChain ) {
      // map chain of expression ASTs into TableData -> TableData closures
      var impChain = [];
      for ( var i = 0; i < expChain.length; i++ ) {
        var exp = expChain[ i ];
        var opImpl = simpleOpImplMap[ exp.operator ];
        var args = exp.args.slice();
        var impFn = opImpl.apply( null, args ); // apply args and get back a TableData -> TableData fn
        impChain.push( impFn );
      }

      function af( tableData ) {
        var ret = tableData;
        for ( var i = 0; i < impChain.length; i++ ) {
          var opf = impChain[ i ];
          ret = opf( ret );
        }
        return ret;
      }

      return af;
    }


    // map from operator names to implementation factories
    var baseOpImplMap = {
      "table": tableRefImpl
    };

    function getBaseOpImpl( exp ) {
      var opImpl = baseOpImplMap[ exp.operator ];

      var args = exp.args.slice();
      var opRes = opImpl.apply( null, args );
      return opRes;
    }

    function evalQuery( queryExp ) {
      var expChain = queryExp._getRep();
      var opImpl = null;

      if ( expChain.length < 1 ) {
        throw new Error( "evalQuery: empty query chain" );
      }

      // TODO: Deal with join(), which will have more than one source table!
      var baseExp = expChain.shift();
      opImpl = getBaseOpImpl( baseExp );

      return opImpl.then( evalSimpleExpChain( expChain ) )
    }

    return {
      "evalQuery": evalQuery, 
    };
  }

  function relTabClient( tableName ) {
    // TODO
    return {};
  }

}(jQuery));