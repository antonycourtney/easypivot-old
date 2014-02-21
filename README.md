# easypivot

## Overview

easypivot is a general purpose pivot table with a web-based UI.

easypivot uses [SlickGrid](http://github.com/mleibman/slickgrid), an advanced JavaScript grid/spreadsheet component, for rendering.  

Internally easypivot uses a layered architecture to communicate with data sources.  At the lowest level, easypivot depends on a library called **reltab** which abstracts the essential functions of a relational database.  Reltab provides a convenient JavaScript API using operator chaining for specifying relational queries, and includes implementation of one reltab *engine* that runs directly in the browser.

