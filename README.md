# easypivot

## Overview

easypivot is a general purpose pivot table with a web-based UI.

easypivot uses [SlickGrid](http://github.com/mleibman/slickgrid), an advanced JavaScript grid/spreadsheet component, for rendering.  

Internally easypivot uses a layered architecture to communicate with data sources.  At the lowest level, easypivot depends on a library called **reltab** which abstracts the essential functions of a relational database.  Reltab provides a convenient JavaScript API using operator chaining for specifying relational queries, and includes implementation of one reltab *engine* that runs directly in the browser.

## Getting Started

In the directory where you've cloned this repository, do:

   $ python -m SimpleHTTPServer

and then open [localhost:8000/eptest.html](http://localhost:8000/eptest.html) in a web browser.  You should see an interactive pivot table of the BART Salary data. It may be informative to look at the console log output to see what's going on, particularly the reltab queries that are generated in response to interactive events on the pivot tree (opening and closing nodes).

Another place to look are the tests: [tests/test.html](http://localhost:8000/tests/test.html) and [tests/aggTreeTest.html](http://localhost:8000/tests/aggTreeTest.html).
They aren't as assertive as they should be yet, but looking at the code should give some idea of the reltab and aggTree APIs.
