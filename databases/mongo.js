const { MongoClient } = require("mongodb");

const config = require("../config.js")

module.exports = async (logging = true, log_prefix = "") => {
    const client = new MongoClient(config.MONGODB_CONSTRING);
    await client.connect();

    // Do some dummy work to ensure connection is established (list all databases)
    const databasesList = await client.db().admin().listDatabases();
    if (logging)
        console.log(log_prefix + `Connected to MongoDB and found ${databasesList.databases.length} databases`);


    return client;
}