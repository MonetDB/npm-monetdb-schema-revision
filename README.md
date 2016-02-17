[![Build Status](https://travis-ci.org/MonetDB/npm-monetdb-schema-revision.svg)](https://travis-ci.org/MonetDB/npm-monetdb-schema-revision)
[![npm version](https://badge.fury.io/js/monetdb-schema-revision.svg)](https://badge.fury.io/js/monetdb-schema-revision)
[![Dependency Status](https://david-dm.org/MonetDB/npm-monetdb-schema-revision.svg)](https://david-dm.org/MonetDB/npm-monetdb-schema-revision)

# MonetDB Schema Revision
This module offers a single function that, given:
- a MonetDB connection
- a directory of SQL files

Brings the database schema in the given connection from its current state to the next state.

### SQL files in the given directory
Every SQL file in your directory should be of the form x.sql, where x is a non-negative integer. 
We use this to convention to be able to automatically detect order in the SQL files. So 3.sql can never
be executed by first executing 1.sql and 2.sql (or only 1.sql, if 2.sql does not exist).

### Current state
Every time this module runs, it stores the current revision in a configurable database table. The current
revision is nothing more than an integer, derived from the filename (so 1.sql -> 1, 23.sql -> 23).
 
### Next state
This module executes all SQL files in your directory, either until there are no more files or until we hit
a limit that you can specify. For example, let's assume you have files 0.sql, 1.sql, and 2.sql in your dir
and the current revision of your database schema is 0. 
- If you then run the code without an upper limit, files 1.sql and 2.sql will be run, leaving you in revision 2. 
- However, if you specify the next state to be revision 1, only 1.sql will be run.
- And, as you would expect, specifying the next state to be revision 0 would do absolutely nothing.



## Example
Some code:

```javascript
// specify database options, see https://github.com/MonetDB/monetdb-nodejs for more details
var dbOptions = {
    dbname: "test"
};

// initialize the function by providing the database options
var schemaRevFn = require("./index.js")(dbOptions);

// Now run the promise returning function by providing it with a root path
// (for more info on the Q promises that we use here, see https://www.npmjs.com/package/q)
schemaRevFn("Tests/sql").then(function(appliedDeltas) {
    console.log(appliedDeltas);

    // Run it again, just for show
    return schemaRevFn("Tests/sql");
}).then(function(appliedDeltas) {
    console.log(appliedDeltas);

    // Make sure we close the database connection now that we will not use it anymore
    schemaRevFn.closeConnection();
});
```

Output of the above program:
```
Could not execute SQL in file Tests/sql/4.sql: Error: 42000!syntax error, unexpected SOME in: "some"
{ '0': true, '1': true, '2': true, '3': true, '4': false }
Could not execute SQL in file Tests/sql/4.sql: Error: 42000!syntax error, unexpected SOME in: "some"
{ '4': false }
```

Note that the errors shown are written from within this module, and denote that the SQL from some file
could not be executed. All the files until this point are however successfully executed, and hence
the promise that is returned by schemaRevFn still resolves. The resolve value is an object that maps
all attempted revision numbers (based on the files found in your directory) to a boolean that indicates
whether or not execution of the file was successful. Hence, you can see that property 4 of the first
object is false, while all others are true. The revision after that action will therefore equal 3.

In case errors occur (like in the example), they are written to stderr.
