// Hive Vars
module.exports.HIVE_NODES = [
    "https://api.hive.blog",
    "https://api.deathwing.me",
    "https://hive-api.arcange.eu",
    "https://hived.emre.sh",
    "https://api.openhive.network"
]

module.exports.PORT = process.env.PORT || 3000;

module.exports.CLIP_API_ADDRESS = process.env.CLIP_API_ADDRESS || "http://localhost:8080";

// process.env.OPENSEARCH_NODES = "['host-node-1:9200', 'host-node-2:9200', ...]"
module.exports.OPENSEARCH_NODES = JSON.parse(process.env.OPENSEARCH_NODES || '[]')
module.exports.OPENSEARCH_AUTH = process.env.OPENSEARCH_AUTH;
module.exports.HEALTHCHECK_INTERVAL = process.env.HEALTHCHECK_INTERVAL || 7500; // in ms

// Opensearch Connection
const opensearch = require('@opensearch-project/opensearch')
module.exports.getOsClient = () => {
    const nodes = JSON.parse(process.env.OPENSEARCH_NODES || "[]");
    const [username, password] = process.env.OPENSEARCH_AUTH.split(':')

    return new opensearch.Client({
        nodes: nodes,
        auth : {username, password},
        ssl: {}
    });
};

// MongoDB Conenction
module.exports.MONGODB_CONSTRING = process.env.MongoDB_Connection_String;
const { MongoClient } = require("mongodb");
module.exports.getMongoClient = () => {
    return new Promise(resolve => {
            MongoClient.connect(process.env.MongoDB_Connection_String, { useUnifiedTopology: true }, (err, db) => {
            if (err) throw err;
            resolve(db);
            });
        })
}

// Redis Cluster Connection
const Redis = require("ioredis");
module.exports.getRedisClient = async () => {
    const nodes = process.env.Redis_Nodes.split(',').map(addr => {
        const [host, port] = addr.split(':');
        return {host, port};
    });
    const cluster = await new Redis.Cluster(
        nodes,
        {
            redisOptions: {
                password : process.env.Redis_Password,
                username : "default"
            },
            enableAutoPipelining: true
        }
    );
      
    let connected = false;
    cluster.on('error', (err) => console.log('Redis Cluster Error', err));
    cluster.on("connect", () => {console.log("Connected to RedisDB"); connected = true;});
    cluster.on('ready', () => console.log('Redis Cluster ready'));
    cluster.on("reconnecting", () => console.log('Redis Cluster reconnecting '));

    while (!connected)
        await new Promise(resolve => setTimeout(resolve, 1000));

    // Connect and Test Cluster with a simple Message (TTL is set to 5 secs)
    await cluster.set('test-cluster', 'success', 'EX', 5);
    const result = await cluster.get('test-cluster');
    if(result !== 'success')
        console.warn('\tRedis Cluster is not working properly', result);
    else
        console.log('\tRedis Cluster is working properly');

    return cluster;
}

// Post ID Hash
const XXHash = require('xxhash');
module.exports.getCommentID = ({author, permlink}) => {
    const buffer = Buffer.from(`${author}/${permlink}`);
    return XXHash.hash64(buffer, 0xCAFEBABE).toString("base64");
}

// Vector-field Sim APIS
module.exports.VECTORFIELD_SIM_APIS = {
    "en" : process.env.VECTOR_SIM_API_EN || "http://hive-comments-last-7d-en.hive-discover.tech",
    "es" : process.env.VECTOR_SIM_API_ES || "http://hive-comments-last-7d-es.hive-discover.tech",
    "avg_image_vector" : process.env.VECTOR_SIM_API_AVG_IMG || "http://hive-comments-last-7d-avg-images.hive-discover.tech"
  }