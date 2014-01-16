(function ($) {
  $.extend(true,window, {
    relTab: {
      local: localRelTab,
      remote: relTabClient,
      query: createQueryExp,
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
  // constructor
  function RelTabQueryExp(inChain, opRep) {
    var _expChain = inChain.slice(); // shallow copy inChain
    if( opRep )
      _expChain.push( opRep );

    function opExpToString() {
      var rs = this.operator + " ( " + JSON.stringify( this.args ) + " )";
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

    function mkFilterExp( fexp ) {
      return mkOperator( "filter", fexp );
    }

    function mkProjectExp( cols ) {
      return mkOperator( "project", cols );
    }

    function mkGroupBy( cols, aggs ) {
      return mkOperator( "groupBy", cols, aggs );
    }

    function mkMapColumns( colMap ) {
      return mkOperator( "mapColumns", colMap );
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
      "filter": mkFilterExp,
      "project": mkProjectExp,
      "toString": toString,
      "groupBy": mkGroupBy,
      "mapColumns": mkMapColumns,
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
        var ns = new Schema( { columns: projectCols, columnMetadata: inSchema.columnMetadata } );

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
      function ff( tableData ) {
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
        this.str = undefined;
      }

      UniqAgg.prototype.mplus = function( val ) {
        if ( typeof val !== "undefined" ) {
          if ( this.initial ) {
            this.str = val;
            this.initial = false; // our first defined val
          } else {
            if( this.str != val )
              this.str = undefined; 
          }
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

      function gb( tableData ) {

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

    function mapColumnsImpl( cmap ) {
      // TODO: check that all columns are columns of original schema,
      // and that applying cmap will not violate any invariants on Schema....but need to nail down
      // exactly what those invariants are first!

      function mc( tableData ) {
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


    var simpleOpImplMap = {
      "filter": filterImpl,
      "project": projectImpl,
      "groupBy": groupByImpl,
      "mapColumns": mapColumnsImpl
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
      if ( !opImpl ) {
        throw new Error( "getBaseOpImpl: unknown operator '" + exp.operator + "'" );
      }
      var args = exp.args.slice();
      var opRes = opImpl.apply( null, args );
      return opRes;
    }

    function evalQuery( queryExp ) {
      var expChain = queryExp._getRep().slice();
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
      "Schema": Schema,
      "evalQuery": evalQuery, 
    };
  }

  function relTabClient( tableName ) {
    // TODO
    return {};
  }

}(jQuery));