(function ($) {
  $.extend(true,window, {
    RelTab: {
      Local: LocalRelTab,
      Remote: RelTabClient,
      Query: createQueryExp,
      Filter: {
        And: createFilterAndExp,
        Or: createFilterOrExp
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
  function LocalRelTab() {
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

    function tableRefImpl( inImpl, tableName ) {

      function getSchema() {

        function onLoad( trep ) {
          return trep.schema;
        }

        var lp = ensureLoaded( tableName );
        return lp.then( onLoad );
      }

      function evalQuery() {

        function onLoad( trep ) {
          return trep.rowData;
        }

        var lp = ensureLoaded( tableName, onLoad );

        return lp.then( onLoad );
      }

      return {
        "getSchema": getSchema,
        "evalQuery": evalQuery
      };
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

    function projectImpl( inImpl, projectCols ) {

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

      function getPsp() {
        if ( !psp ) {
          psp = inImpl.getSchema().then( calcState );
        }
        return psp;
      }

      function getSchema() {
        return getPsp().then( function( so ) {
          return so.schema;
        } );
      };

      function evalQuery() {        

        function onProjectState( ps ) {

          function permuteRowData( rowData ) {
            function permuteOneRow( row ) {
              return d3.permute( row, ps.permutation);
            }
            return rowData.map( permuteOneRow );
          }

          return inImpl.evalQuery().then( permuteRowData );
        }

        return getPsp().then( onProjectState );
      }

      return {
        "args": arguments,
        "getSchema": getSchema,
        "evalQuery": evalQuery
      }
    };


    function filterImpl( inImpl, fexp ) {
      function getSchema() {
        // filter doesn't modify schema; just pass through:
        return inImpl.getSchema();      
      } 

      function evalQuery() {
        // TODO
        throw new Error("not implemented");
      }

      return {
        "args": arguments,
        "getSchema": getSchema,
        "evalQuery": evalQuery
      }
    };

    function groupByImpl( inImpl, cols, aggs ) {
      var sp = null;  // state promise

      console.log( "groupByImpl: cols: ", cols, ", aggs: ", aggs );

      function getSP() {
        if( !sp ) {
          sp = inImpl.evalQuery().then( calcState );
        }

        return sp;
      }

      function calcSchema( inSchema ) {
        var gbs = Object.create( inSchema );
        gbs.columns = cols.concat( aggs ); // TODO: deal with explicitly specified (non-default) aggregations!

        return gbs;
      }

      function getSchema() {
        return inImpl.getSchema().then( calcSchema );
      } 

      function evalQuery() {
        // TODO
      }

      return {
        "args": arguments,
        "getSchema": getSchema,
        "evalQuery": evalQuery
      }
    };



    // map from operator names to implementation factories
    var opImplMap = {
      "table": tableRefImpl,
      "filter": filterImpl,
      "project": projectImpl,
      "groupBy": groupByImpl,
    };

    function getOpImpl( inOpImpl, exp ) {
      var opFactory = opImplMap[ exp.operator ];

      var args = exp.args.slice();
      args.unshift( inOpImpl );

      var opImpl = opFactory.apply( null, args );
      /* var opImpl = Object.create( opCtor.prototype );
       opCtor.apply( opImpl, inOpImpl, exp.args );
      */
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

    function getSchema( queryExp ) {
      var impl = getImpl( queryExp );

      return impl.getSchema();
    }

    function evalQuery(queryExp ) {
      var impl = getImpl( queryExp );

      return impl.evalQuery();
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