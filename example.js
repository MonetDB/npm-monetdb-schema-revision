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

