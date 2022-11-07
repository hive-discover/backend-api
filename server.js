const makeApp = require("./app.js")
const {createOsClient} = require("./databases/opensearch.js")
const createMongoClient = require("./databases/mongo.js")
const config = require('./config.js')

async function main(){
  // Prepare Database Connections
  console.log("Establishing connections...")
  const os_client = config.getOsClient();
  const mongo_client = await createMongoClient(true, "   ")
  // const redis_client = await config.getRedisClient();
  console.log("   ==> All connections established! \n")

  // Create Express App
  console.log("Creating Express App...")
  const app = makeApp(os_client, mongo_client)
  console.log("   ==> Express App created! \n")

  // Start Express App
  console.log("Starting Express App...")
  const server = app.listen(config.PORT, () => {
    console.log(`   ==> Express App listening at http://localhost:${config.PORT}! \n\n\n`)
  });

  return server;
}


main();

// const app = makeApp()

// const port = 3000

// // Establish Mongo Conenction
// config.getMongoClient().then(()=>{
//   console.log("MongoDB Connection established")

//   // Then start the server
//   app.listen(port, () => {
//     console.log(`App listening at http://localhost:${port}`)
//   });
// });