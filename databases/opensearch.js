const opensearch = require('@opensearch-project/opensearch')

const config = require("../config.js")

async function measureNodePerformance(host, n = (process.env.OPENSEARCH_PERFORMANCE_ANALYZER_N || 10)){
  const start_time = process.hrtime();
  let os_client = null;
  let success = false;
  
  for(let i = 0; i < n; i++){
    try{
      // Establish connection and send query n times
      os_client = new opensearch.Client({
        node: 'https://' + config.OPENSEARCH_AUTH + '@' + host,
        ssl: {}
      }); 

      // Send random knn-query and retrieve 10 doc
      const vector = Array.from({length: 300}, () => Math.random());
      const response = await os_client.search({
        index : "hive-post-data", 
        body: {
          "size" : 10,
          "query" : {
            "script_score": {
              "query": {"match_all" : {}},
              "script": {
                "source": "knn_score",
                "lang": "knn",
                "params": {
                  "field": "doc_vector.en",
                  "query_value": vector,
                  "space_type": "cosinesimil"
                }
              }
            }     
          }  
        }
      })

      if(response.body.hits.hits.length === 0)
        throw new Error("No results were retrieved");

      // Performng acknowledgement
      success = true;   
    }catch (e) {
      console.warn("Could not connect to Opensearch Node (" + host + "): " + e.message);
    }
  }

  if(!success)
    return null;

  // Calculate elasped time in seconds (avg)
  const hrtime = process.hrtime(start_time)
  const elapsed_seconds = ((hrtime[0] + (hrtime[1] / 1e9)) / n).toFixed(3);
  return [os_client, host, elapsed_seconds];
}

async function doHealthcheck(os_client){

  // Retrieve just a simple list of tables to check if the client is working
  try{
    const response = await os_client.indices.exists({index : "hive-post-data"});
    if(response.statusCode !== 200)
      throw new Error("Request returned a status code of " + response.statusCode + " instead of 200");
    if(response.body !== true)
      throw new Error("Index hive-post-data does not exist / cannot be found");

    // Successfull: node is alive
  }catch (e) {
    console.error("Opensearch Healthcheck failed: " + e.message);
    process.exit(1);
  }

  // Do it later again
  setTimeout(() => {doHealthcheck(os_client)}, config.HEALTHCHECK_INTERVAL);
}

// Make (best) Connection to an Opensearch Node
async function createOsClient(logging = true, log_prefix = "") {
    if(config.OPENSEARCH_NODES.length === 0)
        throw new Error("No Opensearch Nodes were specified");

    // Iterate through available OS-Nodes and measure response time for a quite heavy workload
    const node_responses = [];
    for(const host of config.OPENSEARCH_NODES){
      const result = await measureNodePerformance(host);
      if(result)
        node_responses.push(result);
    }

    // Sort and select best node
    node_responses.sort((a, b) => a[2] - b[2]);
    if(node_responses.length === 0)
      throw new Error("No Opensearch Nodes were available");
       
    // Do Logging
    if(logging){
      console.log(log_prefix + "Opensearch Nodes:")
      const logFunc = (prefix, result) => console.log(log_prefix + `   - ${prefix} Node: ${result[1]} (avg. ${result[2]}s)`);
      logFunc("Best", node_responses[0]);
      logFunc("Worst", node_responses[node_responses.length - 1]);
    }

    // Start Healthcheck and return best client
    const best_client = node_responses[0][0];
    await doHealthcheck(best_client); // await the healthcheck to ensure the client is working (only one time)
    return best_client;
}

module.exports.createOsClient = createOsClient;
module.exports.measureNodePerformance = measureNodePerformance;
module.exports.doHealthcheck = doHealthcheck;