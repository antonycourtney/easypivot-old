#
# A simple CherryPy based RESTful web server for paged access to a table in a sqlite database
# Developed for csvview but should work for any sqlite table

import cherrypy
from mako.template import Template
from mako.lookup import TemplateLookup
import os
import os.path
import json
import sqlite3
import string
import sys
import tempfile
import threading
import webbrowser

lookup = TemplateLookup(directories=['html'])
threadLocal = threading.local()

def getDbConn( dbName ):
    """Get a thread-local database connection.
    """
    varName = "sqlite3-dbConn-" + dbName
    v = getattr( threadLocal, varName, None )
    if v is None:
        v = sqlite3.connect( dbName )
        setattr( threadLocal, varName, v )
    return v

def getTableInfo( dbConn, tableName ):
    """Use sqlite tableinfo pragma to retrieve metadata on the given table
    """
    query = 'pragma table_info("%s")' % tableName
    c = dbConn.execute( query )
    r = c.fetchall()
    return r

def viewFormat( columnType, cellVal ):
    """ format cellVal suitable for client-side rendering
    """
    if cellVal==None:
        return None
    intFormatStr = "{:,d}"
    realFormatStr = "{:,.2f}"
    if columnType=="integer":
        ret = intFormatStr.format( cellVal )
    elif columnType=="real":
        ret = realFormatStr.format( cellVal )
    else:
        ret = cellVal
    return ret

class PagedDbTable(object):
    def __init__( self, dbName, dbTableName):
        super( PagedDbTable, self ).__init__()
        self.dbName = dbName
        self.dbTableName = dbTableName
        query = 'select count(*) from "' + dbTableName + '"'
        dbConn = getDbConn( dbName )
        c = dbConn.execute( query );
        self.totalRowCount = c.fetchone()[0]
        self.baseQuery = 'select * from "' + dbTableName + '"'
        self.tableInfo = getTableInfo( dbConn, dbTableName )
        self.columnNames = map( lambda ti: ti[1], self.tableInfo )
        self.columnTypes = map( lambda ti: ti[2], self.tableInfo )
        # extract human-friendly descriptions from columnInfo companion table
        qcinfoTable = '"' + dbTableName + '_columnInfo"'
        c = dbConn.execute( "select description from " + qcinfoTable)
        rows = c.fetchall()
        self.columnDescs = map( lambda r:r[0], rows )
        self.columnInfo = []
        for (cn,cd,ct) in zip(self.columnNames,self.columnDescs,self.columnTypes):
            cmap = { 'id': cn, 'field': cn, 'name': cd, 'type': ct }
            self.columnInfo.append( cmap )
        # print self.columnInfo

    def getColumnInfo( self ):
        return self.columnInfo

    def getDataPage( self, sortCol, sortDir, startRow, rowLimit ):
        dbConn = getDbConn( self.dbName )
        if( sortCol != None and sortCol in self.columnNames and sortDir in ['asc','desc']):
            orderStr = ' order by "' + sortCol + '" ' + sortDir
        else:
            orderStr = ""
        query = self.baseQuery + orderStr + " limit " + str( startRow ) + ", " + str( rowLimit )
        # print query
        c = dbConn.execute( query )
        rows = c.fetchall()
        # print " ==> ", len( rows ), " rows"
        
        # now prepare rows for sending to view:
        viewRows = []
        for row in rows:
            mappedRow = { columnName : viewFormat( columnType, cellVal ) for 
                (columnName, columnType, cellVal) in zip( self.columnNames, self.columnTypes, row ) }
            viewRows.append( mappedRow )
#        namedRows = map( lambda r: dict( zip( self.columnNames, r)), rows )
        return viewRows

    def getRowData( self ):
        """ Returns all row data as an array; does not attempt to interpret / format the data.
        Returns data of array of arrays instead of as a dicionary keyed by column name. 
        """
        dbConn = getDbConn( self.dbName )
        query = self.baseQuery
        # print query
        c = dbConn.execute( query )
        rows = c.fetchall()
        print " ==> ", len( rows ), " rows"
        return rows


#
# N.B.:  We're still using a REST-ful (stateless) approach here, but doing so via the default() method.
# We do this because it appears that CherryPy's MethodDispatcher() doesn't allow default() or index()
# methods, which in turn would force us to reify the table hierarchy from the sqllite database as a
# tree of Python objects, which we don't want to do.
# So we just stick with the standard Dispatcher() but this means using default() to provide RESTful
# URIs. 

# Provide RESTful paged access to named table
class TableResource(object):
    def __init__(self, dbName):
        super( TableResource, self ).__init__()
        self.dbName = dbName

    @cherrypy.expose
    def default( self, tableName, startRow = 0, rowLimit = 10, sortby = '' ):
        dbTable = PagedDbTable( self.dbName, tableName )     
        # print "startRow = ", startRow, ", rowLimit = ", rowLimit, ", sortby = '", sortby, "'"
        startRow = int( startRow )
        rowLimit = int( rowLimit )
        sortstr = sortby.strip();
        if( len(sortstr) > 0 ):
            [ sortcol, sortdir ] = sortstr.split('+')
        else:
            [ sortcol, sortdir ] = [ None, None ] 
        cherrypy.response.headers['Content-Type'] = 'application/json'
        # rowData = self.dataFile['data'][ startRow : startRow + rowLimit ]
        columnInfo = dbTable.getColumnInfo()
        rowData = dbTable.getDataPage( sortcol, sortdir, startRow, rowLimit )
        request = { 'startRow': startRow, 'rowLimit': rowLimit }
        response = { 'request': request, 'columnInfo': columnInfo, 
                     'totalRowCount': dbTable.totalRowCount, 'results': rowData }                                                          
        return json.dumps( response )

# Use simple templating to inject table name extracted from request params back in to HTML on client side: 
class TableViewerResource(object):
    def __init__( self, templateFileName):
        self.templateFileName = templateFileName

    @cherrypy.expose
    def default(self, table_name=''):
        return self.to_html( table_name )

    def to_html(self, table_name):
        tmpl = lookup.get_template( self.templateFileName )
        return tmpl.render(table_name=table_name)


APP_DIR = os.path.abspath(".")
config = {'/':
                {'tools.staticdir.on': True,
                 'tools.staticdir.dir': APP_DIR
                },
        }

def open_page(tableName):
    webbrowser.open_new("http://127.0.0.1:8080/table_viewer?table_name=" + tableName )

class Root(object):
    def __init__( self, tables, table_viewer ):
        self.tables = tables
        self.table_viewer = table_viewer

def startWebServer( dbName, tableName, templateFileName ):
    root = Root( TableResource( dbName ), TableViewerResource( templateFileName ) )
    dbTable = PagedDbTable( dbName, tableName )
    cherrypy.config.update( {'log.screen': False })
    cherrypy.engine.subscribe('start', lambda : open_page( tableName ) )
    cherrypy.quickstart( root, '/', config)

def dumpJSON( outFilePath, dbName, tableName ):
    dt = PagedDbTable( dbName, tableName )
    ci = dt.getColumnInfo()
    # For now, let's just extract a schema from columnInfo stored format
    columnMetadata = {}
    for c in ci:
        cEntry = { 'type': c['type'] }
        if c['name']!=c['id']:
            cEntry['displayName'] = c['name']
        columnMetadata[ c['id'] ] = cEntry

    # We call the column identifier 'id' instead of 'name' because current column info format uses 'name' for 'displayName'
    columns = map( lambda c: c['id'], ci ); 
    # schema = map( lambda c: { 'id': c['id'], 'type': c['type'] }, ci );
    rowData = dt.getRowData()
    tableData = [ { 'fileFormatVersion': 1, 
                    'columns': columns,
                    'columnMetadata': columnMetadata, 
                    'totalRowCount': dt.totalRowCount } , 
                  { 'rowData': rowData } ]
    with open(outFilePath,'w') as outFile:
        json.dump( tableData, outFile, indent=2, sort_keys=True )

# simpler Root that serves no table data
class TVRoot(object):
    def __init__( self, table_viewer ):
        self.table_viewer = table_viewer

def serveJSONFile( tableName, templateFileName ):
    root = TVRoot( TableViewerResource( templateFileName ) )
    cherrypy.config.update( {'log.screen': False })
    cherrypy.engine.subscribe('start', lambda : open_page( tableName ) )
    cherrypy.quickstart( root, '/', config)