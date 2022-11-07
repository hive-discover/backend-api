const config = require('./config');
const hivecrypt = require('hivecrypt');
const uuid = require('uuid');
const workerpool = require('workerpool');

const clamp = (value, min, max) => Math.min(Math.max(value, min), max);

module.exports = (os_client, mongo_client) => {

  const documents = require('./documents')(os_client, mongo_client);


  const decryptWorkerPool = workerpool.pool(__dirname + '/workers/decryptMetadata.js', {
    minWorkers : 'max'    
  });
  

  mongo_client = mongo_client.db("activities");

  return (username, prvActivityKey) => {

    async function getUserAverageReadingTime() {
      console.time("getUserAverageReadingTime")
      const pipeline = [
        {
          $match: {
            username: username,
          },
        },
        {
          $group: {
            _id: "$metadata_id",
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
            posts: {
              $sum: 1,
            },
          },
        },
      ];

      const result = await mongo_client.collection("user_has_scrolled").aggregate(pipeline).toArray()

      console.timeEnd("getUserAverageReadingTime")
      return {
        avg: result[0]?.avg || 0,
        posts: result[0]?.posts || 0,
        total: result[0]?.total || 0,
      };
    }   

    async function rankActivities(hits) {
      let activities = {}; // {post-id : timestamp-counting}
      let posts = new Set();

      hits.forEach(({ _id, timestamp_count }) => {
        const post_id = config.getCommentID(_id);
        
        posts.add(_id);
        activities[post_id] = timestamp_count;
      });

      // Get average count of timestamps for these authorperms
      // average_timestamp_counts = {post-id : {avg : 45, users : 534, total : 535}}
      posts = [...posts.values()];
      const [{ avg: user_avg_timestamps }, ...posts_avg_timestamps] =
        await Promise.all([
          getUserAverageReadingTime(),
          ...posts.map((item) => documents.getAverageTimestampCounts(item)),
        ]);

      const average_timestamp_counts = Object.fromEntries(
        posts.map((v, index) => [
          config.getCommentID(posts[index]),
          posts_avg_timestamps[index],
        ])
      );
      // Score of 1, when the user has an equal amount of timestamps compared to the average
      // Score between 0 and 1, when the user has less timestamps than the average
      // Score between 1 and 2, when the user has more timestamps than the average

      // Do the same with the average timestamps of the user
      // Add value to the score:
      //  2, when the user has an equal amount of timestamps compared to his average
      // <2, when the user has less/more timestamps than his average

      // Remap activities to {post-id : [score] }
      activities = Object.fromEntries(
        Object.entries(activities)
          .map(([post_id, timestamps]) => {
            const user_event_counter = parseFloat(timestamps) || 0;
            const avg = parseFloat(average_timestamp_counts[post_id]?.avg) || 0;
            let val = 0.0;

            if (avg > 0 && user_event_counter > 0)
              // global average
              val += clamp(user_event_counter / avg, 0.0, 2.0);
            if (user_avg_timestamps > 0 && user_event_counter > 0)
              // user average
              val +=
                2 *
                (user_event_counter < user_avg_timestamps
                  ? user_event_counter / user_avg_timestamps
                  : user_avg_timestamps / user_event_counter);

            if (val > 0) return [post_id, val];
            return null;
          })
          .filter((item) => item)
      );

      // The most interesting activity is the one with the highest score
      // The max score is 4

      return activities;
    }

    async function getScoredAccountActivities(
      limit = 1000,
      min = 1000,
      allow_filling = true
    ) {

      // Get logged activities
      const pipeline = [
        {
          $match: {
            username: username,
          },
        },
        {
          $group: {
            _id : "$metadata_id",
            timestamps : {
              $push: "$created",
            },
            indexes : {
              $push: "$index",
            },
            metadata : {
              $first: "$metadata",
            },
            created : {
              $first: "$created",
            }
          },
        },
        {
          $addFields: {
            timestamp_count: { $size: "$timestamps" },
            index_min: { $min: "$indexes" },
          },
        },
        {
          $sort: {
            index_min: 1,
          },
        },
        {
          $limit: limit,
        },
        {
          $project: {
            _id: 1,
            metadata: 1,
            timestamp_count: 1,
            index_min : 1,
            created : 1
          }
        }
      ];

      // Iterate over the results and decode the data
      console.time("user_has_scrolled")
      const hits = await mongo_client.collection("user_has_scrolled").aggregate(pipeline).toArray();
      console.timeEnd("user_has_scrolled")
      console.time("decryptMetadata")
      const activity_hits = await Promise.all(hits.map(doc => decryptWorkerPool.exec("decryptMetadata", [prvActivityKey, doc]))).then((result) => result.filter((item) => item));
      console.timeEnd("decryptMetadata")
      console.time("rankActivities")
      const activity_scores = await rankActivities(activity_hits);
      console.timeEnd("rankActivities")

  
      // Fill it with posts by the user (when under min)
      if (Object.keys(activity_scores).length < min && allow_filling) {
        // Score of authored-post is 3
        const diff = min - Object.keys(activity_scores).length;
        result = await os_client.search({
          index: "hive-posts",
          body: {
            size: diff,
            query: {
              bool: {
                must: [{ term: { author: { value: username } } }],
              },
            },
            _source: {
              includes: ["author", "permlink"],
            },
            sort: { timestamp: "DESC" }, // get latest posts first
          },
        });
        result.body.hits.hits.forEach((hit) => {
          activity_scores[config.getCommentID(hit._source)] = 3.0;
        });
      }

      // Fill it with votes (when under min)
      if (Object.keys(activity_scores).length < min && allow_filling) {
        // Vote score is 1.2
        const diff = min - Object.keys(activity_scores).length;
        result = await os_client.search({
          index: "hive-posts",
          body: {
            size: diff,
            query: {
              bool: {
                must: [{ term: { upvotes: { value: username } } }],
              },
            },
            _source: {
              includes: ["author", "permlink"],
            },
            sort: { timestamp: "DESC" }, // get latest posts first
          },
        });
        result.body.hits.hits.forEach((hit) => {
          activity_scores[config.getCommentID(hit._source)] = 1.2;
        });
      }

      return activity_scores;
    }

    async function getAlreadyRecommendedIDs(days_minus = 7){
        // Build and Execute query
        const startDate = new Date(Date.now() - days_minus * 24 * 60 * 60 * 1000);
        const cursor = await mongo_client.collection("user_got_recommended").find({username : username}, {metadata : 1, created : 1}).sort({$natural : -1}).limit(1000);

        // Get all ids from the query by decrypting the metadata and created
        const ids = [];
        while (await cursor.hasNext()) {
            // Get next document and compare with startDate
            const doc = await cursor.next();
            const {_id, created} = await decryptWorkerPool.exec("decryptMetadata", [prvActivityKey, doc]) || {};
            if (created && created < startDate) break;
            
            const post_id = config.getCommentID(_id);
            ids.push(post_id);
        }

        return ids;
    }

    return {
        getScoredAccountActivities,
        getUserAverageReadingTime,
        getAlreadyRecommendedIDs
    }
  };
};

