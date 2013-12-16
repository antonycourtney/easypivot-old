(function ($) {
  $.extend(true,window, {
    RelTab: {
      Local: LocalRelTab,
      Remote: RelTabClient,
      Query: createQueryExp,
      Filter: {
        And: createFilterAndExp,
        Or: createFilterOrExp
      }
    } 
  });

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

    function mkOperator(opName,args)
    {
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
      "_getRep": getRep,
      // "rowCount"
      // "groupBy": mkGroupBy
      // TODO: join
      // "sort"
      // TODO: "extend": mkExtendExp,
      // "crossTab"
    };
  }

  function createQueryExp() {
    return new RelTabQueryExp([]);
  }


  /*
   * Pure client-side implementation of RelTab engine on RelTab JSON data files
   */
  function LocalRelTab() {
    var tableCache = {};

    function Schema( schemaData ) {
      var s = schemaData;

      s.getColumnType = function( colId ) {
        var md = s.columnMetadata[ colId ];

        return md.type;
      }

      return schemaData;  // for now
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
      var tcentry = tableCache[ tableName ];

      if ( tcentry ) {
        if ( tcentry.status == "loaded" ) {
          cbfn( null, tcentry.tableRep ); // cache hit
        } else if ( tcentry.status == "pending" ) {
          tcentry.callbacks.push( cbfn ); // just put ourselves on queue of callbacks
        } else {
          throw new Error( "unexpected table cache status: '" + tcentry.status + "'" );
        }
      } else {
        var url = "../json/" + tableName + ".json";        
        tcentry = { status: "pending", callbacks: [ cbfn ] };
        tableCache[ tableName ] = tcentry;

        // Callback invoked with raw JSON table data
        function onLoad( error, tableData ) {
          var tableRep = null;

          console.log( "ensureLoaded:onLoad: ", error, tableData );
          if( error ) {
            console.log( "ensureLoaded error: ", error );
            tcentry.status = "error";
            tcentry.error = error;
          } else {
            tableRep = new TableRep( tableData );
            tcentry.status = "loaded";
            tcentry.tableRep = tableRep;
          }

          for ( var i = 0; i < tcentry.callbacks.length; i++ ) {
            tcentry.callbacks[i]( error, tableRep );
          }
          tcentry.callbacks = [];
        }

        console.log( "ensureLoaded: sending request for " + url );
        d3.json( url, onLoad );
      }
    }

    function TableRefImpl( inImpl, tableName ) {

      function getSchema( cbfn ) {

        function onLoad( error, table ) {
          var s = null;
          if( !error )
            s = table.schema;

          cbfn( error, s );
        }

        ensureLoaded( tableName, onLoad );
      }

      function evalQuery( cbfn ) {
        function onLoad( error, table ) {
          var rowData = null;

          if ( !error )
            rowData = table.rowData;

          cbfn( error, rowData );
        }

        ensureLoaded( tableName, onLoad );

      }

      return {
        "getSchema": getSchema,
        "evalQuery": evalQuery
      };
    }

    /*
     * TODO: We need to rethink this a bit.
     * Issues:
     *   - We should try to do some lazy eval so we can re-use results.
     *   - Probably have some amount of validating processing (like checking of columns for validity)
     *     that should happen whether operation is getSchema or evalQuery.
     */
    function ProjectImpl( inImpl, projectCols ) {
      function getSchema( cbfn ) {
        function onGetSchema( error, schema ) {
          if (error) {
            cbfn( err, null );
            return;
          }
          // ensure all columns in projectCols in schema:
          for ( var i = 0 ; i < projectCols.length; i++ ) {
            var colId = projectCols[ i ];
            if( !( schema.columnMetadata[ colId ] ) ) {
              err = new Error( "project: unknown column Id '" + colId + "'" );
              cbfn( err, null );
              return;
            }
          }
          var ns = Object.create( schema );
          ns.columns = projectCols;

          cbfn( error, ns );
        }

        inImpl.getSchema( onGetSchema );      
      } 

      function evalQuery( cbfn ) {
        throw new Error( "not implemented yet" );
      }

      return {
        "args": arguments,
        "getSchema": getSchema,
        "evalQuery": evalQuery
      }
    };


    function SelectImpl( inImpl, fexp ) {
      function getSchema( cbfn ) {
        // select doesn't modify schema; just pass through:
        inImpl.getSchema( cbfn );      
      } 

      function evalQuery( cbfn ) {
        // TODO
      }

      return {
        "args": arguments,
        "getSchema": getSchema,
        "evalQuery": evalQuery
      }
    };



    function loadTable(tableName,cbfn) {
      var url = "json/" + tableName + ".json";

      function onGet(error, urlData) {
        // TODO: error handling!
        console.log( urlData );
        data = urlData;
        dataLoaded = true;

        cbfn( this );
      }

      d3.json(url, onGet);  // TODO / FIXME: Perhaps drop d3 dependency
    };

    // map from operator names to implementation constructors
    var opImplMap = {
      "table": TableRefImpl,
      "select": SelectImpl,
      "project": ProjectImpl
    };

    function getOpImpl( inOpImpl, exp ) {
      var opCtor = opImplMap[ exp.operator ];
      var opImpl = new opCtor( inOpImpl, exp.args );
      return opImpl;
    }

    function getImpl( queryExp ) {
      var expChain = queryExp._getRep();
      var opImpl = null;

      /*
       * Potential optimization:  Could build a smarter tree based on what operators we see in expression chain.
       * Example:  we could eliminate dynamic indirections on getSchema() for operators that don't modify their input
       * schema.
       */
      for ( i=0; i < expChain.length; i++ ) {
        var e = expChain[i];
        opImpl = getOpImpl( opImpl, e );
      }
      return opImpl;
    }

    function getSchema(queryExp,cbfn) {
      var impl = getImpl( queryExp );

      impl.getSchema( cbfn );
    }

    function evalQuery(queryExp,cbfn) {
      var impl = getImpl( queryExp );

      impl.evalQuery( cbfn );
    }

    return {
      "getSchema": getSchema,
      "evalQuery": evalQuery, 
    };
  }

  function RelTabClient( tableName ) {
    // TODO
    return {};
  }

}(jQuery));