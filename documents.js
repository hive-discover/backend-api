const axios = require('axios');
const { VECTORFIELD_SIM_APIS } = require('./config');

module.exports = (os_client, mongo_client) => {

  async function getAverageTimestampCounts({ author, permlink }) {
    const pipeline = [
      {
        $match: {
          "metadata.author": author,
          "metadata.permlink": permlink
        },
      },
      {
        $group: {
          _id: "$userID",
          count: {
            $sum: 1,
          },
        },
      },
      {
        $group: {
          _id: "result",
          avg: {
            $avg: "$count",
          },
          total: {
            $sum: "$count",
          },
          users: {
            $sum: 1,
          },
        },
      },
    ];

    const result = await mongo_client
      .db("activities")
      .collection("post_is_scrolled")
      .aggregate(pipeline)
      .toArray();

    return {
      avg: result[0]?.avg || 0,
      users: result[0]?.users || 0,
      total: result[0]?.total || 0,
    };
  }

  async function findSimilarByVectors(binary_vectors, k, vector_field){
    if(binary_vectors.length === 0) return [];
    if(k <= 0) return [];
    if(VECTORFIELD_SIM_APIS[vector_field] === undefined) return [];

    // Encode vectors to base64 and create a query
    const query = {
      k : k,
      vectors : binary_vectors.map(v => Buffer.from(v.buffer).toString('base64'))
    }

    // Call the API
    const response = await axios.post(VECTORFIELD_SIM_APIS[vector_field] + "/search", query);
    if(response.status !== 200) throw new Error("Network error while searching for similar vectors: " + response.statusText);
    if(response.data.status !== "ok") throw new Error("Error while searching for similar vectors: " + response.data.message);

    // Remap data.results from [ [[id0, x0], [id1, x1], ...], ... ] to [ {id : x}, ... ]
    const results = response.data.results.map(result => {
      return result.reduce((acc, item) => {
        return {...acc, [item[0]] : item[1]};
      }, {});
    });
    
    return results;
  }

  return {
    findSimilarByVectors,
    getAverageTimestampCounts,
  }

};
