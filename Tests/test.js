var path = require("path");

var chai = require("chai");
var chaiAsPromised = require("chai-as-promised");
var Q = require("q");

var should = chai.should();
chai.use(chaiAsPromised);

var MDB = require("monetdb")();

function dbOptions() {
    return {
        dbname: "test"
    };
}

function schemaRev(dbOpts) {
    return require("../index.js")(dbOpts ? dbOpts : dbOptions(), true);
}

var sqlDir = path.join(__dirname, "sql");


var schemaRevisionFn = null;
var conn = null;
function init() {
    schemaRevisionFn = schemaRev();
    conn = new MDB(dbOptions());
    return conn.connect();
}

function cleanup() {
    return conn.query(
        "SELECT tables.name\n" +
        "FROM schemas JOIN tables ON schemas.id = tables.schema_id\n" +
        "WHERE schemas.name = 'sys'\n" +
        "  AND tables.system = FALSE"
    ).then(function(result) {
        var dropQs = result.data.map(function(table) {
            return conn.query("DROP TABLE \"" + table + "\"");
        });
        return Q.all(dropQs).then(function() {
            return conn.close();
        }).then(function() {
            conn = null;
            schemaRevisionFn = null;
        });
    });
}

function createRevTable() {
    return conn.query("CREATE TABLE schema_revision (cur_rev INTEGER)");
}


describe("# Passing dbOptions", function() {
    it("should throw exception on no dbOptions", function() {
        (function () {
            require("../index.js")();
        }).should.throw(Error);
    });

    it("should throw exception on incomplete dbOptions", function() {
        (function () {
            require("../index.js")({});
        }).should.throw(Error);
    });

    it("should not throw exception on proper dbOptions", function() {
        (function () {
            require("../index.js")(dbOptions());
        }).should.not.throw(Error);
    });
});




describe("# Unit tests for private functions", function() {
    beforeEach(init);
    afterEach(cleanup);

    describe("## _getSchemaName()", function() {
        it("should return the sys schema when no schema was given", function() {
            schemaRevisionFn._getSchemaName().should.equal("sys");
        });

        it("should return the defaultSchema if that was provided", function() {
            var dbOpts = dbOptions();
            dbOpts.defaultSchema = "some_schema";
            var customSchemaRevisionFn = schemaRev(dbOpts);
            customSchemaRevisionFn._getSchemaName().should.equal("some_schema");
        });
    });

    describe("## _tableExists(table)", function() {
        it("should resolve to false for non-existing table", function() {
            return schemaRevisionFn._tableExists("non_existant")
                .should.eventually.equal(false);
        });

        it("should resolve to true for existing table", function() {
            return conn.query("CREATE TABLE a (a INT)").then(function() {
                return schemaRevisionFn._tableExists("a");
            }).should.eventually.equal(true);
        });
    });

    describe("## _execSqlFromFile(file)", function() {
        it("should fail on non-existing file", function() {
            return schemaRevisionFn._execSqlFromFile("/non/existing/file.sql")
                .should.be.rejected;
        });

        it("should fail on file with erroneous SQL", function() {
            return schemaRevisionFn._execSqlFromFile(path.join(sqlDir, "invalid.sql"))
                .should.be.rejected;
        });

        it("should execute SQL on existing file", function() {
            return schemaRevisionFn._execSqlFromFile(path.join(sqlDir, "0.sql")).then(function() {
                return conn.query("SELECT * FROM a");
            });
        });
    });

    describe("## _getCurrentRevision(revisionTable)", function() {
        it("should return -1 if revision table does not exist", function() {
            return schemaRevisionFn._getCurrentRevision("schema_revision").should.eventually.equal(-1);
        });

        it("should return -1 if revision table exists but has no records", function() {
            return createRevTable().then(function() {
                return schemaRevisionFn._getCurrentRevision("schema_revision");
            }).should.eventually.equal(-1);
        });

        it("should return 5 if 5 was inserted into the revision table", function() {
            createRevTable().then(function() {
                return conn.query("INSERT INTO schema_revision VALUES (?)", [5]);
            }).then(function() {
                return schemaRevisionFn._getCurrentRevision("schema_revision");
            }).should.eventually.equal(5);
        });
    });

    describe("## _updateCurrentRevision(revisionTable, revision)", function() {
        it("should fail on wrong revision table", function() {
            return schemaRevisionFn._updateCurrentRevision("non_existant", 5)
                .should.be.rejected;
        });

        it("should fail on wrong revision number type", function() {
            return createRevTable().then(function() {
                return schemaRevisionFn._updateCurrentRevision("schema_revision", "wrong_type")
            }).should.be.rejected;
        });

        it("should pass on valid revision number", function() {
            return createRevTable().then(function() {
                return schemaRevisionFn._updateCurrentRevision("schema_revision", 42)
            }).then(function() {
                return conn.query("SELECT * FROM schema_revision");
            }).should.eventually.have.property("data")
                .that.deep.equals([[42]]);
        });
    });

    describe("## _getSortedDeltaNrs(root, curRev[ ,untilRev])", function() {
        it("should fail on non-existing root", function() {
            return schemaRevisionFn._getSortedDeltaNrs("/non/existant/root", -1)
                .should.be.rejected;
        });

        it("should give [1,2,3,4] with curRev = 0", function() {
            return schemaRevisionFn._getSortedDeltaNrs(sqlDir, 0)
                .should.eventually.deep.equal([1, 2, 3, 4]);
        });

        it("should give [1,2] with curRev = 0 and untilRev = 2", function() {
            return schemaRevisionFn._getSortedDeltaNrs(sqlDir, 0, 2)
                .should.eventually.deep.equal([1, 2]);
        });

        it("should give [2,3] with curRev = 1 and untilRev = 3", function() {
            return schemaRevisionFn._getSortedDeltaNrs(sqlDir, 1, 3)
                .should.eventually.deep.equal([2, 3]);
        });

        it("should give [2] with curRev = 1 and untilRev = 2", function() {
            return schemaRevisionFn._getSortedDeltaNrs(sqlDir, 1, 2)
                .should.eventually.deep.equal([2]);
        });

        it("should give [] with curRev = 1 and untilRev = 1", function() {
            return schemaRevisionFn._getSortedDeltaNrs(sqlDir, 1, 1)
                .should.eventually.deep.equal([]);
        });
    });
});


describe("# Functional tests: running the main function entirely", function() {
    beforeEach(init);
    afterEach(cleanup);

    function tableExists(table, desired) {
        return schemaRevisionFn._tableExists(table).then(function(exists) {
            if(exists != desired) {
                throw new Error("Table '" + table + "' " +
                    (desired ? "should exist but does not.." : " should not exist but does..")
                );
            }
        })
    }

    function revisionNumberEquals(revisionNr) {
        return schemaRevisionFn._getCurrentRevision("schema_revision").then(function(revNr) {
            if(revisionNr != revNr) {
                throw new Error("Revision number expected to equal " + revisionNr + " but actual value was " + revNr);
            }
        });
    }

    function nothingIsApplied() {
        return Q.all([
            revisionNumberEquals(-1),
            tableExists("a", false),
            tableExists("b", false),
            tableExists("c", false)
        ]);
    }

    function only0IsApplied() {
        var a = conn.query("SELECT * FROM a");
        var b = conn.query("SELECT * FROM b");
        return Q.all([
            revisionNumberEquals(0),
            a.should.eventually.have.property("rows", 1),
            a.should.eventually.have.property("data")
                .that.deep.equals([[1, "FOO", 1.2]]),
            b.should.eventually.have.property("rows", 0),
            tableExists("c", false)
        ]);
    }

    function until3IsApplied() {
        var a = conn.query("SELECT * FROM a");
        var c = conn.query("SELECT * FROM c");

        return Q.all([
            revisionNumberEquals(3),
            a.should.eventually.have.property("rows", 2),
            a.should.eventually.have.property("data")
                .that.deep.equals([[1, 1.2, null], [1, 1.2, 42]]),
            tableExists("b", false),
            c.should.eventually.have.property("rows", 1),
            c.should.eventually.have.property("data")
                .that.deep.equals([["SomeVal"]])
        ]);
    }

    it("should stick to revision 3 on encountering invalid file 4.sql", function() {
        return schemaRevisionFn(sqlDir).then(function(appliedDeltas) {
            appliedDeltas.should.deep.equal({0: true, 1: true, 2: true, 3: true, 4: false});
            return until3IsApplied();
        });
    });


    it("should apply everything up until delta 3 if untilDelta is 3", function() {
        return schemaRevisionFn(sqlDir, {untilRev: 3}).then(function(appliedDeltas) {
            appliedDeltas.should.deep.equal({0: true, 1: true, 2: true, 3: true});
            return until3IsApplied();
        });
    });

    it("should apply only delta 0 if {untilRev:0} is given", function() {
        return schemaRevisionFn(sqlDir, {untilRev: 0}).then(function(appliedDeltas) {
            appliedDeltas.should.deep.equal({0: true});
            return only0IsApplied();
        });
    });

    it("should apply everything up until delta 3 if we take steps [{untilRev:0}, {untilRev:1}, {untilRev:1}, {untilRev:2}, {untilRev:3}, {untilRev:5}]", function() {
        return schemaRevisionFn(sqlDir, {untilRev: 0}).then(function(appliedDeltas) {
            appliedDeltas.should.deep.equal({0: true});
            return schemaRevisionFn(sqlDir, {untilRev: 1});
        }).then(function(appliedDeltas) {
            appliedDeltas.should.deep.equal({1: true});
            return schemaRevisionFn(sqlDir, {untilRev: 1});
        }).then(function(appliedDeltas) {
            appliedDeltas.should.deep.equal({});
            return schemaRevisionFn(sqlDir, {untilRev: 2});
        }).then(function(appliedDeltas) {
            appliedDeltas.should.deep.equal({2: true});
            return schemaRevisionFn(sqlDir, {untilRev: 3});
        }).then(function(appliedDeltas) {
            appliedDeltas.should.deep.equal({3: true});
            return schemaRevisionFn(sqlDir, {untilRev: 5});
        }).then(function(appliedDeltas) {
            appliedDeltas.should.deep.equal({4: false});
            return until3IsApplied();
        });
    });

    it("should apply nothing if {untilRev:-1} is given", function() {
        return schemaRevisionFn(sqlDir, {untilRev: -1}).then(function(appliedDeltas) {
            appliedDeltas.should.deep.equal({});
            return nothingIsApplied();
        });
    });
});


