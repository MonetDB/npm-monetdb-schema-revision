// This module applies all available SQL deltas to an existing database
var path = require("path");
var fs = require("fs");

var Q = require("q");

module.exports = function(dbOptions, testing) {
    var conn, connPromise;

    if(dbOptions.conn) {
        conn = dbOptions.conn;
        connPromise = Q.when(true);
    } else {
        var MDB = require("monetdb")();
        conn = new MDB(dbOptions);
        connPromise = conn.connect();
    }


    function _getSchemaName() {
        return dbOptions.defaultSchema || "sys";
    }

    function _tableExists(table) {
        return conn.query(
            "SELECT schemas.name, tables.name\n" +
            "FROM sys.schemas JOIN sys.tables ON schemas.id = tables.schema_id\n" +
            "WHERE schemas.name = ? AND tables.name = ?",
            [_getSchemaName(), table]
        ).then(function(result) {
            return result.rows > 0;
        });
    }

    function _execSqlFromFile(file) {
        return Q.nfcall(fs.readFile, file).then(function(sql) {
            return conn.query(sql.toString());
        });
    }

    function _getCurrentRevision(revTable) {
        return _tableExists(revTable).then(function(tExists) {
            if(!tExists) {
                return -1;
            }
            return conn.query("SELECT cur_rev FROM " + revTable, false).then(function(result) {
                return result.rows ? result.data[0][0] : -1;
            });
        });
    }

    function _updateCurrentRevision(revTable, revNr) {
        return conn.query("SELECT * FROM \"" + revTable + "\"").then(function(result) {
            if(!result.rows) {
                return conn.query("INSERT INTO \"" + revTable + "\" VALUES (?)", [revNr]);
            }
            return conn.query("UPDATE \"" + revTable + "\" SET cur_rev = ?", [revNr]);
        });
    }

    // Get all delta numbers that need to be applied to the current schema to reach the state defined by
    // untilRev (or all delta files starting from the current revision if untilRev is not provided)
    // Note that this function does not return the delta file with a name equal to the provided curRev,
    // since the current schema should already include that delta file.
    // Note: the output will be sorted on revision number in ascending fashion, eg [45,46,47]
    function _getSortedDeltaNrs(root, curRev, untilRev) {
        return Q.nfcall(fs.readdir, root).then(function(files) {
            return files.map(function(file) {
                return parseInt(file.slice(0, -4));
            }).filter(function(revNr) {
                return !isNaN(revNr) && revNr > curRev && (isNaN(untilRev) || revNr <= untilRev);
            }).sort(function(a, b) {
                return a - b;
            });
        });
    }






    // root [string]: The absolute or relative path to the root of your SQL files
    // Possible options:
    // - untilRev [integer]: This will be the last revision number that will be applied.
    // - revTable [string]: The database table in which the current revision will be stored.
    var x = function(root, opts) {
        var options = (typeof opts == "object") ? opts : {};
        var revTable = options.revTable || "schema_revision";

        return connPromise.then(function() {
            // Wait for connect promise to return, to ensure that connecting does not fail
            return _tableExists(revTable);
        }).then(function(exists) {
            // Create rev table if it does not exist yet
            if(!exists) {
                return conn.query("CREATE TABLE \"" + revTable + "\" (cur_rev INTEGER)");
            }
        }).then(function() {
            // get current revision
            return _getCurrentRevision(revTable);
        }).then(function(curRevNr) {
            // Use the current rev and the possibly provided untilRev to get the delta files
            // that need to be executed to go from the current state to the desired state.
            return _getSortedDeltaNrs(root, curRevNr, options.untilRev);
        }).then(function(deltaNrs) {
            // mapping of delta number to boolean indicating success or failure
            var appliedDeltas = deltaNrs.reduce(function(o, deltaNr) {
                o[deltaNr] = false;
                return o;
            }, {});

            // Execute the SQL in all of the files, one at a time, and update the current revision
            // after every run file, so after every file we have a consistent state. In case of
            // failure, we could then just pick up where we left.
            var stop = false;
            return deltaNrs.reduce(function(promise, deltaNr) {
                if(stop) return promise;
                var sqlPath = path.join(root, deltaNr + ".sql");
                return promise.then(function() {
                    return _execSqlFromFile(sqlPath).then(function() {
                        appliedDeltas[deltaNr] = true;
                    }, function(err) {
                        process.stderr.write("Could not execute SQL in file " + sqlPath + ": " + err + "\n");
                        stop = true;
                    });
                });
            }, Q.when(true)).then(function() {
                return appliedDeltas;
            });
        }).then(function(appliedDeltas) {
            var lastUpdatedDelta = null;
            Object.keys(appliedDeltas).forEach(function(deltaNr) {
                appliedDeltas[deltaNr] && (lastUpdatedDelta = parseInt(deltaNr));
            });
            if(lastUpdatedDelta !== null) {
                return _updateCurrentRevision(revTable, lastUpdatedDelta).then(function() {
                    return appliedDeltas;
                }, function(err) {
                    process.stderr.write("Could not update the current revision: " + err + ". Please do this update manually.\n");
                    return appliedDeltas;
                });
            }
            return appliedDeltas;
        });
    };

    x.closeConnection = function() {
        conn && conn.close();
        conn = null;
    };

    if(testing) {
        x._getSchemaName = _getSchemaName;
        x._tableExists = _tableExists;
        x._execSqlFromFile = _execSqlFromFile;
        x._getCurrentRevision = _getCurrentRevision;
        x._updateCurrentRevision = _updateCurrentRevision;
        x._getSortedDeltaNrs = _getSortedDeltaNrs;
    }

    return x;
};
