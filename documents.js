const axios = require('axios');

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

  async function getAuthorPermlinks(ids){
    if(!ids || ids.length === 0) return [];

    // Get all author/permlink combinations for the given ids
    const query = {
      "size" : ids.length,
      "query" : {
        "ids" : {
          "values" : ids
        }
      },
      "_source" : {
        "includes" : [ "author", "permlink" ]
      }
    }
    const result = await os_client.search({index : "hive-posts", body : query});

    // Map author/permlink combinations into the ids list
    result.body.hits.hits.forEach(hit => {
      const id = hit._id;
      const author = hit._source.author;
      const permlink = hit._source.permlink;

      for(let i = 0; i < ids.length; i++){
        if(ids[i] === id){
          ids[i] = {author, permlink};
          break;
        }
      }
    });

    return ids.filter(x => (x.author && x.permlink));
  }

  async function getDocVectors(ids){
    if(!ids || ids.length === 0) return [];

    // Get all author/permlink combinations for the given ids
    const query = {
      "size" : ids.length,
      "query" : {
        "ids" : {
          "values" : ids
        }
      },
      "_source" : {
        "includes" : [ "doc_vector" ]
      }
    }
    const result = await os_client.search({index : "hive-posts", body : query});

    // remap to {id : doc_vector} and filter none values out
    const vectors = result.body.hits.hits.reduce((acc, hit) => {
      const id = hit._id;
      const doc_vector = hit._source.doc_vector;
      return {...acc, [id]: doc_vector};
    } ,{});

    return vectors;
  }

  async function findSimilarToDocVectors(doc_vectors, k, username, tags, parent_permlinks, filter_out_ids){
    const pre_query = {
      bool : {
        filter : [
          {
            bool : {
              // Add other comparisons to the main filter
              must_not : [
                // Post must not be written by user or liked by user or in filter_out_ids
                {term : {'author' : {value : username}}},
                {term : {'upvotes' : {value : username}}}
              ]
                .concat(filter_out_ids?.length > 0 ? [{ids : {values : filter_out_ids}}] : []),
              must : []
                .concat(tags?.length > 0 ? [{terms : {tags : tags}}] : []) // add maybe tags
                .concat(parent_permlinks?.length > 0 ? [{terms : {parent_permlink : parent_permlinks}}] : []) // add maybe parent_permlinks
            }
          }
        ]
      }
    };

    // New Try: multiple requests
    const requests = [];
    Object.values(doc_vectors).map(doc_item => {
      // Do search for each doc_vector in each language
      const full_body = Object.keys(doc_item).map(lang => {
        if(!lang || !doc_item[lang]) return;

        return [
          {index : "hive-posts-last-7d"},
          {
            "size" : k,
            "query" : {
              "script_score": {
                "query": pre_query,
                "script": {
                  "source": "knn_score",
                  "lang": "knn",
                  "params": {
                    "field": "doc_vector." + lang,
                    "query_value": doc_item[lang],
                    "space_type": "cosinesimil"
                  }
                }
              }     
            },
            "_source" : {
              "includes" : ["author", "permlink", "doc_vector"]
            }    
          }
        ]

      }).flat(1).filter(item => item != null);
      
      const request = os_client.msearch({
        index : "hive-posts-last-7d",
        body : full_body
      });
      requests.push(request);
    });

    const responses = await Promise.all(requests).then(responses => responses.flat(1));
    const results = responses.map(response => response.body.responses).flat(1);
    const docs = results.map(r => {
      return r.hits.hits.map(hit => {
        return {
          author : hit._source.author,
          permlink : hit._source.permlink,
          doc_vector : hit._source.doc_vector
        }
      });
    });

    return docs.flat(1);
  }

  async function findSimilarPosts(ids, k, username, tags = [], parent_permlink = [], filter_out_ids = []){
    // Add ids to the filter_out_ids and remove duplicates
    filter_out_ids = [...new Set([...(filter_out_ids), ...ids])];

    const requestBody = {
      limit : k,
      usernames : [username],
      out_ids : filter_out_ids,
    }
    tags.forEach(tag => requestBody.tags ? requestBody.tags.push(tag) : requestBody.tags = [tag]);
    parent_permlink.forEach(pl => requestBody.parent_permlinks ? requestBody.parent_permlinks.push(pl) : requestBody.parent_permlinks = [pl]);

    const responses = await Promise.all(ids.map(async (id) => {
      return await axios.post("http://feed-making.hive-discover.tech/similar-by-id", {...requestBody, post_id : id})
        .then(response => response?.data && response.status === 200 ? response.data : null)
        .catch(error => {console.error("Error while requesting similar posts for id " + id, error); return null;});
    }));

    // fitler out none values, flat and remove dups values
    let results = responses.flat(1).filter(x => x);
    results = [...new Set(results)];
    return results;
  }

  async function calcSimilarScores(source_id, target_ids){
    if(!Array.isArray(target_ids))
      target_ids = [target_ids];
    if(target_ids.length === 0 || !source_id)
      return [];

    const result = await axios.post("http://feed-making.hive-discover.tech/calc-sim-scores", {source_id, target_ids})
      .then(response => response?.data && response.status === 200 ? response.data : null)
      .then(result => {
        if(Array.isArray(result))
          return result; 
        throw new Error("Invalid result from calc-sim-scores");
      })
      .catch(error => {console.error("Error while requesting similar scores for id " + id, error); return [];});

    return result;
  }

  return {
    findSimilarPosts,
    getAverageTimestampCounts,
    getAuthorPermlinks,
    getDocVectors,
    findSimilarToDocVectors,
    calcSimilarScores
  }

};
