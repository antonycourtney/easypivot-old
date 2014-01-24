(function ($) {
  $.extend(true,window, {
    relTab: {
      local: localRelTab,
      remote: relTabClient,
      query: {
        table: createTableQueryExp
      },
      // filter support:
      and: createFilterAndExp,
      or: createFilterOrExp,

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
   */

   /* Constructor function */
  function RelTabFilterExp(boolOp) {
    var _exp = {
      boolOp: boolOp,
      args: []  // array of conjunctions / disjuncts
    };

    function mkRelOp( relOp, lhs, rhs ) {
      return {
        type: "RelOp",
        relOp: relOp,
        lhs: lhs,
        rhs: rhs
      };
    }

    var ppOpMap = {
      "eq": "=",
      "gt": ">",
      "ge": ">=",
      "lt": "<",
      "le": "<="
    };

    function ppRelOp(exp) {
      var s = exp.lhs + ppOpMap[exp.relOp] + exp.rhs;
      return s;
    }

    function ppSubExp(subExp) {
      var s = "( " + subExp.exp.toSqlWhere() + " )";
      return s;
    }

    var ppFuncs = {
      RelOp: ppRelOp,
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

    function mkEq(lhs,rhs) {
      _exp.args.push( mkRelOp( "eq", lhs, rhs ) );
      return this;
    }

    function mkGt(lhs,rhs) {
      _exp.args.push( mkRelOp( "gt", lhs, rhs ) );
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
      },
      "toJSON": function() {
        return { filterExp: _exp };
      },
      "_getRep": function() {return _exp;},
    };
  }  

  function createFilterAndExp() {
    return new RelTabFilterExp("and");
  }

  function createFilterOrExp() {
    return new RelTabFilterExp("or");
  }

  /* AST rep for query expressions.
   * Expressions formed by chaining method calls on an initially
   * empty query.
   * Each call to ctor appends opRep to _expChain.
   * Constructor not exposed in user API
   */
  function mkOpRep( opName, valArgs, tableArgs )
  {
    function opRepToString() {
      var rs = this.operator + "( [ " + this.tableArgs.toString() + " ], " + JSON.stringify( this.valArgs ) + " )";
      return rs;
    }
    var opRep = { operator: opName, tableArgs: tableArgs, valArgs: valArgs, toString: opRepToString };
    return opRep;
  };

  function mkQueryExp( opRep ) {

    function mkOperator( opName, valArgs, /* optional */ tableArgs ) {
      tableArgs = tableArgs || [];
      tableArgs.unshift( opRep );
      var nextRep = mkOpRep( opName, valArgs, tableArgs );
      return mkQueryExp( nextRep );
    }

    function mkFilterExp( fexp ) {
      return mkOperator( "filter", [ fexp ] );
    } 

    function mkProjectExp( cols ) {
      return mkOperator( "project", [ cols ] );
    }

    function mkGroupBy( cols, aggs ) {
      return mkOperator( "groupBy", [ cols, aggs ] );
    }

    function mkMapColumns( colMap ) {
      return mkOperator( "mapColumns", [ colMap ] );
    }
    function mkMapColumnsByIndex( colMap ) {
      return mkOperator( "mapColumnsByIndex", [ colMap ] );
    }
    function mkExtend( cols, colMetadata, colValues ) {
      return mkOperator( "extend", [ cols, colMetadata, colValues ] );
    }

    // extendColumn -- convenience for extending just a single column:
    function mkExtendColumn( colId, colmd, val ) {
      var mdMap = {}; 
      var valMap = {};
      mdMap[ colId ] = colmd;
      valMap[ colId ] = val;
      return mkOperator( "extend", [ [ colId ], mdMap, valMap ] );
    }

    function mkConcat( qexp ) {
      return mkOperator( "concat", [], [ qexp._rep ] );
    }

    function toString() {
      return opRep.toString();
    }

    return {
      "_rep": opRep,
      "filter": mkFilterExp,
      "project": mkProjectExp,
      "toString": toString,
      "groupBy": mkGroupBy,
      "mapColumns": mkMapColumns,
      "mapColumnsByIndex": mkMapColumnsByIndex,
      "extend": mkExtend,
      "extendColumn": mkExtendColumn,
      "concat": mkConcat,
    };   
  }

  // Create base of a query expression chain by starting with "table":
  function createTableQueryExp( tableName ) {
    var opRep = mkOpRep( "table", [ tableName ], [] );
    var ret = mkQueryExp( opRep );
    return ret;
  }


  /*
   * Pure client-side implementation of RelTab engine on RelTab JSON data files
   */
  function localRelTab() {
    var tableCache = {};

    /*
     * Terminology:  
     *   A *Column* is a sequence of scalar values of a particular type, along with metadata
     *   describing the column and how it should be rendered (such as the type of scalar values and display
     *   name to use in the column header).
     *   A *Column Id* uniquely identifies a particular column.  A column id is also used as
     *   an index into the column metadata dictionary.
     *   A *Table* is a sequence of columns.
     *   A *Schema* is a sequence of Column Ids and a map from Column Id to Column Metadata info.
     *   A *Column Index* is an ordinal index identifying the i-th column in a Table.
     */
    function Schema( schemaData ) {
      var s = schemaData;

      s.getColumnType = function( colId ) {
        var md = s.columnMetadata[ colId ];

        return md.type;
      }

      s.displayName = function( colId ) {
        var dn = s.columnMetadata[ colId ].displayName || colId;

        return dn;
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

      s.compatCheck = function( sb ) {
        if( this.columns.length != sb.columns.length ) {
          throw new SchemaError( "incompatible schema: columns length mismatch", this, sb );
        }
        for( var i = 0; i < this.columns.length; i++ ) {
          var colId = this.columns[ i ];
          var bColId = sb.columns[ i ];
          if( colId !== bColId ) {
            throw new SchemaError( "incompatible schema: expected '" + colId + "', found '" + bColId + "'", this, sb );
          }
          var colType = this.columnMetadata[ colId ].type;
          var bColType = sb.columnMetadata[ bColId ].type;
          if( colType !== bColType ) {
            throw new SchemaError( "mismatched column types for col '" + colId + "': " + colType + ", " + bColType, this, sb );
          }
        }
        // success!
      }

      return s;  // for now
    }

    function SchemaError( message, s1, s2 ) {
      this.message = message;
      this.s1 = s1;
      this.s2 = s2;
    }

    SchemaError.prototype = new Error();


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
        var ns = new Schema( { columns: projectCols, columnMetadata: inSchema.columnMetadata } );

        return { "schema": ns, "permutation": perm };
      }

      function pf( subTables ) {
        var tableData = subTables[ 0 ];

        var ps = calcState( tableData.schema );
        function permuteOneRow( row ) {
          return d3.permute( row, ps.permutation);
        }
        var outRowData = tableData.rowData.map( permuteOneRow );

        return { "schema": ps.schema, "rowData": outRowData };
      }

      return pf;
    };

    /*
     * compile the given filter expression with rest to the given schema
     */
    function compileFilterExp( schema, fexp ) {
      // base expression (token) types:
      var TOK_IDENT = 0;  // identifier
      var TOK_STR = 1; // string literal
      var TOK_INT = 2;

      var identRE = /[a-zA-Z][a-zA-Z0-9]*/;
      var strRE = /'([^']*)'/;  // TODO: deal with escaped quotes
      var intRE = /[0-9]+/;

      function exactMatch( re, target ) {
        var res = re.exec( target );
        if ( res && (res[0].length==target.length) && res.index==0 )
          return res;
        return null;
      }

      function tokenize( str ) {
        var ret = undefined;
        var match;
        if( match = exactMatch( identRE, str ) ) {
          ret = { tt: TOK_IDENT, val: str }
        } else if( match = exactMatch( strRE, str ) ) {
          ret = { tt: TOK_STR, val: match[1] }
        } else if( match = exactMatch( intRE, str ) ) {
          ret = { tt: TOK_INT, val: parseInt( str ) }
        } else {
          throw new Error( "tokenize: unrecognized token [" + str + "]" );
        }
        return ret;
      }

      function compileAccessor( tok ) {
        var af = undefined;
        if( tok.tt == TOK_IDENT ) {
          var idx = schema.columnIndex( tok.val );
          if( typeof idx == "undefined" ) {
            throw new Error( "compiling filter expression: Unknown column identifier '" + tok.val + "'" );
          }
          af = function( row ) {
            return row[ idx ];
          }
        } else {
          af = function( row ) {
            return tok.val;
          }
        }
        return af;
      }

      var relOpFnMap = {
        "eq": function( l, r ) { return l==r; },
      }

      function compileRelOp( relop ) {
        var tlhs = tokenize( relop.lhs );
        var trhs = tokenize( relop.rhs );
        var lhsef = compileAccessor( tlhs );
        var rhsef = compileAccessor( trhs );
        var cmpFn = relOpFnMap[ relop.relOp ];
        if( !cmpFn ) {
          throw new Error( "compileRelOp: unknown relational operator '" + relop.op + "'" );
        }

        function rf( row ) {
          var lval = lhsef( row );
          var rval = rhsef( row );
          return cmpFn( lval, rval );
        } 
        return rf;
      }

      function compileSimpleExp( se ) {
        if( se.type=="RelOp" ) {
          return compileRelOp( se );
        } else if( se.type=="subexp" ) {
          return compileExp( se.exp );
        } else {
          throw new Error( "error compile simple expression " + JSON.stringify( se ) + ": unknown expr type");
        }
      }

      function compileAndExp( argExps ) {
        var argCFs = argExps.map( compileSimpleExp );

        function cf( row ) {
          for ( i = 0; i < argCFs.length; i++ ) {
            var acf = argCFs[ i ];
            var ret = acf( row );
            if( !ret )
              return false;
          }
          return true;
        } 
        return cf;
      }
      
      function compileOrExp( argExps ) {
        // TODO
      }

      function compileExp( exp ) {
        var rep = exp._getRep();
        var boolOp = rep.boolOp;  // top level
        var cfn = undefined;
        if( boolOp == "and" )
          cfn = compileAndExp
        else
          cfn = compileOrExp;

        return cfn( rep.args );
      } 

      return {
        "evalFilterExp": compileExp( fexp )
      };

    }

    function filterImpl( fexp ) {
      function ff( subTables ) {
        var tableData = subTables[ 0 ];
        console.log( "ff: ", fexp );

        var ce = compileFilterExp( tableData.schema, fexp );

        outRows = [];
        for( var i = 0; i < tableData.rowData.length; i++ ) {
          row = tableData.rowData[ i ];
          if( ce.evalFilterExp( row ) )
            outRows.push( row );
        }

        return { schema: tableData.schema, rowData: outRows };
      }

      return ff;
    };

    // A simple op is a function from a full evaluated query result { schema, rowData } -> { schema, rowData }
    // This can easily be wrapped to make it async / promise-based / caching
    function groupByImpl( cols, aggs ) {
      var aggCols = aggs;  // TODO: deal with explicitly specified (non-default) aggregations!

      function calcSchema( inSchema ) {
        var gbCols = cols.concat( aggCols );
        var gbs = new Schema( { columns: gbCols, columnMetadata: inSchema.columnMetadata } );

        return gbs;
      }

      function fillArray(value, len) {
        var arr = [];
        for (var i = 0; i < len; i++) {
          arr.push(value);
        };
        return arr;
      }

      function SumAgg() {
        this.sum = 0;
      }

      SumAgg.prototype.mplus = function( val ) {
        if ( typeof val !== "undefined" )
          this.sum += val;

        return this;
      }

      SumAgg.prototype.finalize = function() {
        return this.sum;
      }

      function UniqAgg() {
        this.initial = true;        
        this.str = null;
      }

      UniqAgg.prototype.mplus = function( val ) {
        if ( this.initial && val != null ) {
          // this is our first non-null value:
          this.str = val;
          this.initial = false;
        } else {
          if( this.str != val )
            this.str = null; 
        }
      }
      UniqAgg.prototype.finalize = function() {
        return this.str;
      }

      function AvgAgg() {
        this.count = 0;
        this.sum = 0;
      }

      AvgAgg.prototype.mplus = function( val ) {
        if ( typeof val !== "undefined" ) {
          this.count++;
          this.sum += val;
        }
        return this;
      }
      AvgAgg.prototype.finalize = function() {
        if ( this.count == 0 )
          return NaN;
        return this.sum / this.count;
      }

      // map of constructors for agg operators:
      var aggMap = {
        "uniq": UniqAgg,
        "sum": SumAgg,
        "avg": AvgAgg, 
      }

      // map from column type to default agg functions:
      var defaultAggs = {
        "integer": SumAgg,
        "text": UniqAgg
      }

      function gb( subTables ) {
        var tableData = subTables[ 0 ];

        console.log( "gb: enter: cols.length ", cols.length );

        var inSchema = tableData.schema;
        var outSchema = calcSchema( inSchema );

        var aggCols = aggs; // TODO: deal with explicitly specified (non-default) aggregations!

        var groupMap = {};
        var keyPerm = calcProjectionPermutation( inSchema, cols );
        var aggColsPerm = calcProjectionPermutation( inSchema, aggCols );

        // construct and return an an array of aggregation objects appropriate
        // to each agg fn and agg column passed to groupBy

        function mkAggAccs() {
          var aggAccs = [];
          for ( var i = 0; i < aggCols.length; i++ ) {
            // TODO: check for typeof aggs[i] == array and use specified agg
            var aggColType = inSchema.columnMetadata[ aggCols[ i ] ].type;
            var aggCtor = defaultAggs[ aggColType ];
            var accObj = new aggCtor();
            aggAccs.push( accObj );
          }
          return aggAccs;
        }


        for ( var i = 0; i < tableData.rowData.length; i++ ) {
          var inRow = tableData.rowData[ i ];

          var keyData = d3.permute( inRow, keyPerm );
          var aggInData = d3.permute( inRow, aggColsPerm );
          var keyStr = JSON.stringify( keyData );

          var groupRow = groupMap[ keyStr ];
          var aggAccs = undefined;
          if ( !groupRow ) {
            // console.log( "Adding new group for key '" + keyStr + "'");
            aggAccs = mkAggAccs();
            // make an entry in our map:
            groupRow = keyData.concat( aggAccs );
            groupMap[ keyStr ] = groupRow;            
          }
          for ( var j = keyData.length; j < groupRow.length; j++ ) {
            var acc = groupRow[ j ];
            acc.mplus( aggInData[ j - keyData.length ] );
          }
        }  

        // finalize!
        rowData = [];
        for ( keyStr in groupMap ) {
          if ( groupMap.hasOwnProperty( keyStr ) ) {
            groupRow = groupMap[ keyStr ];
            keyData = groupRow.slice( 0, cols.length );
            for ( var j = keyData.length; j < groupRow.length; j++ ) {
              groupRow[ j ] = groupRow[ j ].finalize();                
            }
            rowData.push( groupRow );
          }
        }

        console.log( "gb: exit" );
        return { schema: outSchema, rowData: rowData };
      }

      return gb;
    }

    /*
     * map the display name or type of columns.
     * TODO: perhaps split this into different functions since most operations are only schema transformations,
     * but type mapping will involve touching all input data.
     */
    function mapColumnsImpl( cmap ) {
      // TODO: check that all columns are columns of original schema,
      // and that applying cmap will not violate any invariants on Schema....but need to nail down
      // exactly what those invariants are first!

      function mc( tableData ) {
        var tableData = subTables[ 0 ];
        var inSchema = tableData.schema;

        var outColumns = [];
        var outMetadata = {};
        for ( var i = 0; i < inSchema.columns.length; i++ ) {
          var inColumnId = inSchema.columns[ i ];
          var inColumnInfo = inSchema.columnMetadata[ inColumnId ];
          var cmapColumnInfo = cmap[ inColumnId ];
          if( typeof cmapColumnInfo == "undefined" ) {
            outColumns.push( inColumnId );
            outMetadata[ inColumnId ] = inColumnInfo;
          } else {
            var outColumnId = cmapColumnInfo.id;
            if( typeof outColumnId == "undefined" ) {
              outColId = inColId;
            }

            // Form outColumnfInfo from inColumnInfo and all non-id keys in cmapColumnInfo:
            var outColumnInfo = JSON.parse( JSON.stringify( inColumnInfo ) );
            for( var key in cmapColumnInfo ) {
              if( key!='id' && cmapColumnInfo.hasOwnProperty( key ) )
                outColumnInfo[ key ] = cmapColumnInfo[ key ];
            }
            outMetadata[ outColumnId ] = outColumnInfo;
            outColumns.push( outColumnId );
          }
        }
        var outSchema = new Schema( { columns: outColumns, columnMetadata: outMetadata } );

        // TODO: remap types as needed!

        return { schema: outSchema, rowData: tableData.rowData };
      }

      return mc;
    }

    function mapColumnsByIndexImpl( cmap ) {
      // TODO: try to unify with mapColumns.  Probably means mapColumns will construct an argument to
      // mapColumnsByIndex and use this impl
      function mc( subTables ) {
        var tableData = subTables[ 0 ];
        var inSchema = tableData.schema;

        var outColumns = [];
        var outMetadata = {};
        for ( var inIndex = 0; inIndex < inSchema.columns.length; inIndex++ ) {
          var inColumnId = inSchema.columns[ inIndex ];
          var inColumnInfo = inSchema.columnMetadata[ inColumnId ];
          var cmapColumnInfo = cmap[ inIndex ];
          if( typeof cmapColumnInfo == "undefined" ) {
            outColumns.push( inColumnId );
            outMetadata[ inColumnId ] = inColumnInfo;
          } else {
            var outColumnId = cmapColumnInfo.id;
            if( typeof outColumnId == "undefined" ) {
              outColumnId = inColumnId;
            }

            // Form outColumnfInfo from inColumnInfo and all non-id keys in cmapColumnInfo:
            var outColumnInfo = JSON.parse( JSON.stringify( inColumnInfo ) );
            for( var key in cmapColumnInfo ) {
              if( key!='id' && cmapColumnInfo.hasOwnProperty( key ) )
                outColumnInfo[ key ] = cmapColumnInfo[ key ];
            }
            outMetadata[ outColumnId ] = outColumnInfo;
            outColumns.push( outColumnId );
          }
        }
        var outSchema = new Schema( { columns: outColumns, columnMetadata: outMetadata } );

        // TODO: remap types as needed!

        return { schema: outSchema, rowData: tableData.rowData };
      }

      return mc;
    }

    /*
     * extend a RelTab by adding a column with a constant value.
     */
    function extendImpl( columns, columnMetadata, columnValues ) {

      /*
       * TODO: What are the semantics of doing an extend on a column that already exists?  Decide and spec. it!
       */
      function ef( subTables ) {
        var tableData = subTables[ 0 ];
        var inSchema = tableData.schema;

        var outCols = inSchema.columns.concat( columns );
        var outMetadata = $.extend( {}, inSchema.columnMetadata, columnMetadata );
        var outSchema = new Schema( { columns: outCols, columnMetadata: outMetadata } );

        var extValues = [];
        for( var i = 0; i < columns.length; i++ ) {
          var colId = columns[ i ];
          var val = columnValues && columnValues[ colId ];
          if ( typeof val == "undefined" )
            val = null;
          extValues.push( val );
        }

        var outRows = [];
        for( i = 0; i < tableData.rowData.length; i++ ) {
          var inRow = tableData.rowData[ i ];
          var outRow = inRow.concat( extValues );
          outRows.push( outRow );
        }

        return { schema: outSchema, rowData: outRows };
      }

      return ef;
    }

    /*
     * concat tables
     */
    function concatImpl() {
      function cf( subTables ) {
        var tbl = subTables[ 0 ];
        var res = { schema: tbl.schema, rowData: tbl.rowData };
        for ( i = 1; i < subTables.length; i++ ) {
          tbl = subTables[ i ];
          // check schema compatibility:
          res.schema.compatCheck( tbl.schema );

          res.rowData = res.rowData.concat( tbl.rowData );
        }

        return res;
      }

      return cf;
    }      

    var simpleOpImplMap = {
      "filter": filterImpl,
      "project": projectImpl,
      "groupBy": groupByImpl,
      "mapColumns": mapColumnsImpl,
      "mapColumnsByIndex": mapColumnsByIndexImpl,
      "extend": extendImpl,
      "concat": concatImpl,
    }


    /*
     * Evaluate a non-base expression from its sub-tables
     */
   function evalExpr( opRep, subTables ) {
    var opImpl = simpleOpImplMap[ opRep.operator ];
    var valArgs = opRep.valArgs.slice();
    var impFn = opImpl.apply( null, valArgs );
    var tres = impFn( subTables );
    return tres;      
   }   



    // base expressions:  Do not have any sub-table arguments, and produce a promise<TableData>
    var baseOpImplMap = {
      "table": tableRefImpl
    };

    function evalBaseExpr( exp ) {
      var opImpl = baseOpImplMap[ exp.operator ];
      if ( !opImpl ) {
        throw new Error( "evalBaseExpr: unknown primitive table operator '" + exp.operator + "'" );
      }
      var args = exp.valArgs.slice();
      var opRes = opImpl.apply( null, args );
      return opRes;
    }

    /* evaluate the specified table value in the CSE Map.
     * Returns: promise for the result value
     */
    function evalCSEMap( cseMap, tableId ) {
      var resp = cseMap.promises[ tableId ];
      if( typeof resp == "undefined" ) {
        // no entry yet, make one:
        var opRep = cseMap.valExps[ tableId ];

        var subTables = []; // array of promises:

        if( opRep.tableNums.length > 0 ) {
          // dfs eval of sub-tables:
          subTables = opRep.tableNums.map( function( tid ) { return evalCSEMap( cseMap, tid ); } );
          resp = Q.all( subTables ).then( function( tvals ) { return evalExpr( opRep, tvals ); } );
        } else {
          resp = evalBaseExpr( opRep );
        }
        cseMap.promises[ tableId ] = resp;
      }
      return resp;
    }

    /*
     * use simple depth-first traversal and value numbering in cseMap to
     * identify common table subexpressions.
     */
    function buildCSEMap( cseMap, opRep ) {
      if( typeof opRep == "undefined" ) debugger;
      var tableNums = opRep.tableArgs.map( function( e ) { return buildCSEMap( cseMap, e ); } );
      var expKey = opRep.operator + "( [ " + tableNums.toString() + " ], " + JSON.stringify( opRep.valArgs ) + " )";
      var valNum = cseMap.invMap[ expKey ];
      if( typeof valNum == "undefined" ) {
        // no entry, need to add it:
        // let's use opRep as prototype, and put tableNums in the new object:
        var expRep = Object.create( opRep );
        expRep.tableNums = tableNums;
        var valNum = cseMap.valExps.length;
        cseMap.valExps[ valNum ] = expRep;
        cseMap.invMap[ expKey ] = valNum;
      } // else: cache hit! nothing to do

      return valNum;
    }


    function evalQuery( queryExp ) {
      // use value numbering to build up a map of common subexpression and then evaluate that
      var cseMap = { invMap: {}, valExps: [], promises: [] };
      var queryIdent = buildCSEMap( cseMap, queryExp._rep );     

      console.log( "evalQuery after buildCSEMap: queryIdent: ", queryIdent, ", map: ", cseMap );

      return evalCSEMap( cseMap, queryIdent );

      var opRep = cseMap.valExps[ queryIdent ];  

      return null;
    }

    return {
      "Schema": Schema,
      "evalQuery": evalQuery, 
    };
  }

  function relTabClient( tableName ) {
    // TODO
    return {};
  }

}(jQuery));