const similarity = require( 'compute-cosine-similarity' );
const XXHash = require('xxhash');
const config = require('./config');

const getCommentID = ({author, permlink}) => {
  const buffer = Buffer.from(`${author}/${permlink}`);
  return XXHash.hash64(buffer, 0xCAFEBABE).toString("base64");
}

// const os_client = config.getOsClient();
// const mongo_client = config.getMongoClient().then(client => client.db('activities'));

module.exports = (os_client, mongo_client) => {

  mongo_client = mongo_client.db('activities'); 

  async function getAverageTimestampCounts({author, permlink}){
      const pipeline = [
        {
          '$match': {
            'metadata.author': author, 
            'metadata.permlink': permlink,
            'author' : {'$ne' : author}
          }
        }, {
          '$group': {
            '_id': '$username', 
            'count': {
              '$sum': 1
            }
          }
        }, {
          '$group': {
            '_id': 'result', 
            'avg': {
              '$avg': '$count'
            }, 
            'total': {
              '$sum': '$count'
            }, 
            'users': {
              '$sum': 1
            }
          }
        }
      ]

      const result = await mongo_client.then(client => client.collection('post_scrolled').aggregate(pipeline).toArray());
      return {
        avg : result[0]?.avg || 0,
        users : result[0]?.users || 0,
        total : result[0]?.total || 0
      }
  }

  async function getUserAverageReadingTime(username){
    const pipeline = [
      {
        '$match': {
          'username': username
        }
      }, {
        '$group': {
          '_id': '$metadata', 
          'count': {
            '$sum': 1
          }
        }
      }, {
        '$group': {
          '_id': 'result', 
          'avg': {
            '$avg': '$count'
          }, 
          'total': {
            '$sum': '$count'
          }, 
          'posts': {
            '$sum': 1
          }
        }
      }
    ];

    const result = await mongo_client.then(client => client.collection('post_scrolled').aggregate(pipeline).toArray());

    return {
      avg : result[0]?.avg || 0,
      posts : result[0]?.posts || 0,
      total : result[0]?.total || 0
    }
  }

  function clamp(value, min, max) {
    return Math.min(Math.max(value, min), max);
  }

  async function rankActivities(username, hits){
      let activities = {}; // {post-id : timestamp-counting}
      let posts = new Set();

      hits.forEach(({_id, timestamp_count}) => {
          const post_id = getCommentID(_id);
          posts.add(_id);

          activities[post_id] = timestamp_count;
      });

      // Get average count of timestamps for these authorperms
      // average_timestamp_counts = {post-id : {avg : 45, users : 534, total : 535}}
      posts = [...posts.values()];
      const [{avg : user_avg_timestamps}, ...posts_avg_timestamps] = await Promise.all([
        getUserAverageReadingTime(username),
        ...posts.map(item => getAverageTimestampCounts(item))   
      ]);

      const average_timestamp_counts = Object.fromEntries(posts.map((v, index) => [getCommentID(posts[index]), posts_avg_timestamps[index]]));
      // Score of 1, when the user has an equal amount of timestamps compared to the average
      // Score between 0 and 1, when the user has less timestamps than the average
      // Score between 1 and 2, when the user has more timestamps than the average

      // Do the same with the average timestamps of the user
      // Add value to the score:
      //  2, when the user has an equal amount of timestamps compared to his average
      // <2, when the user has less/more timestamps than his average

      // Remap activities to {post-id : [score] }
      activities = Object.fromEntries(Object.entries(activities).map(([post_id, timestamps]) => {
        const user_event_counter = parseFloat(timestamps) || 0;
        const avg = parseFloat(average_timestamp_counts[post_id]?.avg) || 0;
        let val = 0.0;
        
        if(avg > 0 && user_event_counter > 0) // global average
          val +=  clamp(user_event_counter / avg, 0.0, 2.0);
        if(user_avg_timestamps > 0 && user_event_counter > 0) // user average
          val += 2 * (user_event_counter < user_avg_timestamps ? (user_event_counter / user_avg_timestamps) : (user_avg_timestamps / user_event_counter));
        
        if(val > 0)
          return [post_id,val];
        return null;
      }).filter(item => item));

      // The most interesting activity is the one with the highest score 
      // The max score is 4

      return activities;
  }

  async function getScoredAccountActivities(username, limit = 1000, min = 1000, allow_filling = true){
      let activity_scores = {} // {'post_id': score}

      // Get logged activities
      const pipeline = [
        {
          '$match': {
            'username': username
          }
        }, 
        {
          '$group': {
            '_id': '$metadata', 
            'timestamps' : {
              '$push': '$created'
            }
          }
        },
        {
          "$addFields": {
            "last" : {"$last": "$timestamps"},
            "timestamp_count" : {"$size": "$timestamps"}
          }
        },
        {
          "$sort" : {
            "last" : -1
          }
        },
        {
          "$limit" : limit
        },
        {
          "$project": {
            "timestamps" : 0
          }
        }
      ]; 
      const activity_hits = await mongo_client.then(client => client.collection('post_scrolled').aggregate(pipeline).toArray());
      activity_scores = await rankActivities(username, activity_hits);

      // Fill it with posts by the user (when under min)
      if(Object.keys(activity_scores).length < min && allow_filling){
        // Score of authored-post is 3
        const diff = min - Object.keys(activity_scores).length;
        result = await os_client.search({
            index : "hive-posts",
            body : {
                size : diff,
                query : {
                    bool : {
                        must : [
                            {term : {'author' : {value : username}}}
                          ]
                    }
                },
                _source : {
                  includes : ["nothing-only-id"]
                },
                sort : {timestamp : "DESC"} // get latest posts first
            }
        });
        result.body.hits.hits.forEach(hit => {
            activity_scores[hit._id] = 3.0;
        });
      }

      // Fill it with votes (when under min)
      if(Object.keys(activity_scores).length < min && allow_filling){
        // Vote score is 1.2
        const diff = min - Object.keys(activity_scores).length;
        result = await os_client.search({
          index : "hive-posts",
          body : {
            size : diff,
            query : {
              bool : {
                must : [
                  {term : {'upvotes' : {value : username}}}
                ]
                }
              },
              _source : {
                includes : ["nothing-only-id"]
              },
              sort : {timestamp : "DESC"} // get latest posts first
            }
        });
        result.body.hits.hits.forEach(hit => {
          activity_scores[hit._id] = 1.2;
        });
      }

      return activity_scores;
  }

  function sampleRandomWeighted(weighted_ids, n = 0){
    // Randomly sort weighted
    // StackOverflow Answer to a question: https://stackoverflow.com/a/65207342/7586306
    //  ==> Perform Exponential Distribution

    // weighted_ids = [id, weight]
    weighted_ids = weighted_ids.map(v => [v[0], Math.log10(1- Math.random()) / v[1]]);
    // lowest is to choose
    weighted_ids.sort((a, b) => (a[1] < b[1]) ? -1 : 1);
    
    if(n > 0)
      return weighted_ids.slice(0, n);
    return weighted_ids;
  }

  async function getDocVectors(ids){
    if(!ids || ids.length === 0) return {};

    const result = await os_client.mget({
      index : "hive-posts",
      body : {
        docs : ids.map(id => ({_id : id, _source : ["doc_vector"]}))
      }
    });

    // remap to {id : doc_vector} and filter none values out
    const id_vec_pair = result.body.docs.map(doc => [doc?._id, doc?._source?.doc_vector]);
    return Object.fromEntries(id_vec_pair.filter(item => item[0] != null && item[1] != null));
  }

  async function getAuthorPermlinks(ids){
    if(!ids || ids.length === 0) return [];

    const result = await os_client.mget({
      index : "hive-posts",
      body : {
        docs : ids.map(id => ({_id : id, _source : ["author", "permlink"]}))
      }
    });

    const authorperms = result.body.docs.map(doc => doc?._source?.author ? [{author : doc._source.author, permlink : doc._source.permlink}] : null)
                                        .filter(item => item != null);
    return authorperms.flat();
  }

  async function findSimilarToDocVectors(doc_vectors, k, username, tags, parent_permlinks, filter_out_ids){
    // Build Query
    const pre_query = {
      bool : {
        must_not : [
          // Post must not be written by user or liked by user or in filter_out_ids
          {term : {'author' : {value : username}}},
          {term : {'upvotes' : {value : username}}}
        ].concat(filter_out_ids?.length > 0 ? [{ids : {values : filter_out_ids}}] : []),
        must : []
          .concat(tags?.length > 0 ? [{terms : {tags : tags}}] : []) // add maybe tags
          .concat(parent_permlinks?.length > 0 ? [{terms : {parent_permlink : parent_permlinks}}] : []) // add maybe parent_permlinks
      }
    };
    const full_body = Object.values(doc_vectors).map(doc_item => {
      return Object.keys(doc_item).map(lang => {
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
      }).filter(item => item != null);
    }).flat(2);

    // Find k most similar vectors
    const result = await os_client.msearch({
      index : "hive-posts-last-7d",
      body : full_body
    })

    return result.body.responses.map(response => {
      return response.hits.hits.map(hit => {
        return {
          author : hit._source.author,
          permlink : hit._source.permlink,
          doc_vector : hit._source.doc_vector
        }
      });
    });
  }

  const getFeed = async (account_username, amount, abstraction_value, tags, parent_permlinks, wanted_langs, sample_batch_size=45, account_activity_limit=1000) => {

    // 1. Get the user's activities
    const activity_scores = await getScoredAccountActivities(account_username, account_activity_limit, sample_batch_size + 10);

    // 2. Get a random sample of activities (weighted by the interest-score) and keep the original score
    const sample_ids = sampleRandomWeighted(Object.entries(activity_scores), sample_batch_size).map(v => [v[0], activity_scores[v[0]]]);

    // 3. Get the doc-vectors of the sample
    const sample_doc_vectors = await getDocVectors(sample_ids.map(v => v[0]));

    // 3.1 Remove not-wanted languages
    if(wanted_langs?.length > 0){
      Object.keys(sample_doc_vectors).forEach(id => {
        Object.keys(sample_doc_vectors[id]).forEach(lang => {
          if(!wanted_langs.includes(lang))
            delete sample_doc_vectors[id][lang];
        });
      });
    }

    // 4. Select the k most similar vectors
    const similar_posts = await findSimilarToDocVectors(
      sample_doc_vectors, 
      2 + abstraction_value, 
      account_username, 
      tags, 
      parent_permlinks,
      Object.keys(activity_scores) // filter his activities out
    ).then(x => x.flat());

    // 5. calculate the score of the similar posts with the total cosine similarity score with the sample-ones
    const similar_posts_scores = similar_posts.map(post => {
      const post_id = getCommentID(post);

      // Calculate cosine-similarity
      // (average calculation can be ignored because it is everytime the same divisor: sample_batch_size)
      let total_sims = [];
      Object.keys(post?.doc_vector || {}).forEach(lang => {
        total_sims.push(
          Object.values(sample_doc_vectors).map(doc_item => {
            // Calculate sample-similar-cosine-similarity for this language
            if(!doc_item[lang] || !post.doc_vector[lang]) return 0;

            // TODO: select if this similarity is good or bad
            return similarity(doc_item[lang], post.doc_vector[lang]) + 1;
          }).reduce((a, b) => a + b, 0)
        ); 
      });
    
      return total_sims.map(sim => [post_id, sim]);
    }).flat()
      .filter(item => item[1] > 0)
      // Remove duplicated posts and keep the item with the highest score
      .reduce((acc, item) => {
        acc[item[0]] = (acc[item[0]] || 0) < item[1] ? item[1] : acc[item[0]];
        return acc;
      }, {});


    // 6. select the right amount of posts randomly weighted by the score
    const selected_ids = sampleRandomWeighted(Object.entries(similar_posts_scores), amount).map(v => v[0]);

    // 7. get the authorperms of the selected posts
    const selected_posts = await getAuthorPermlinks(selected_ids);

    return selected_posts;
  }

  return {
    getFeed,
    getScoredAccountActivities,
    getAuthorPermlinks,
    getCommentID,
    getUserAverageReadingTime
  }
}

// module.exports.getFeed = getFeed;
// module.exports.getScoredAccountActivities = getScoredAccountActivities;
// module.exports.getAuthorPermlinks = getAuthorPermlinks
// module.exports.getCommentID = getCommentID;
// module.exports.getUserAverageReadingTime = getUserAverageReadingTime;