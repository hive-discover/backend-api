const config = require('./config');
const hivecrypt = require('hivecrypt');
const uuid = require('uuid');
const workerpool = require('workerpool');

const clamp = (value, min, max) => Math.min(Math.max(value, min), max);

module.exports = (os_client, raw_mongo_client) => {

  const documents = require('./documents')(os_client, raw_mongo_client);


  const decryptWorkerPool = workerpool.pool(__dirname + '/workers/decryptMetadata.js', {
    minWorkers : 'max'    
  });
  
  mongo_client = raw_mongo_client.db("activities");
  mongo_hivedb = raw_mongo_client.db("hive");

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

    async function rankScrollActivities(hits) {
      if(hits.length === 0) return {};

      let activities = {}; // {post-id : timestamp-counting}
      let posts = new Set();

      // get all post ids of the user
      const authorperms = hits.map(({_id : meta}) => ({author : meta.author, permlink : meta.permlink}));
      const post_ids = await mongo_hivedb.collection("comments").find({$or : authorperms}).project({_id : 1, author : 1, permlink : 1}).toArray();


      // Enter all hits into activities
      hits.forEach(({ _id : meta, timestamp_count }) => {
        // Find the post id of the hit
        const db_post = post_ids.find(({author, permlink}) => author === meta.author && permlink === meta.permlink);
        if(!db_post) return;
        
        // Add the hit to the activities and post-list
        posts.add({...meta, _id : db_post._id});
        activities[db_post._id] = timestamp_count;
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
          v._id,
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
            const post_avg = parseFloat(average_timestamp_counts[post_id]?.avg) || 0;
            let val = 0.0;

            if (post_avg > 0 && user_event_counter > 0)
              // global average
              val += clamp(user_event_counter / post_avg, 0.0, 2.0);
            if (user_avg_timestamps > 0 && user_event_counter > 0)
              // user average
              val += clamp(user_event_counter / user_avg_timestamps, 0.0, 2.0)

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
      allow_filling = true,
      with_survey_answers = true
    ) {

      const getScrollActivities = async () => {
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
        const activity_scores = await rankScrollActivities(activity_hits);
        console.timeEnd("rankActivities")

        return activity_scores;
      };

      const getRatingActivities = async () => {
        // Get logged activities
        const cursor = mongo_client.collection("user_has_survey_answered").find(({username})).sort({index : 1}).limit(limit);
        const hits = await cursor.toArray();
        if(!hits.length) return {};

        // Iterate over the results and decode the data
        let activity_hits = await Promise.all(hits.map(doc => decryptWorkerPool.exec("decryptMetadata", [prvActivityKey, doc]))).then((result) => result.filter((item) => item));
        activity_hits = activity_hits.map(({_id : meta}) => ({author : meta.author, permlink : meta.permlink, rating : meta.survey_answer}))

        // Get the post ids
        const authorperms = activity_hits.map(({author, permlink}) => ({author, permlink}));
        const post_ids = await mongo_hivedb.collection("comments").find({$or : authorperms}).project({_id : 1, author : 1, permlink : 1}).toArray();

        // Score the activities with a value between 0 and 4
        // max. survey answer is 5, so we divide by 5 and multiply by 4
        const activity_scores = {};
        activity_hits.forEach((hit) => {
          const post_id = post_ids.find(({author, permlink}) => author === hit.author && permlink === hit.permlink)?._id;
          if (!post_id) return;

          const score = 4 * hit.rating / 5;
          activity_scores[post_id] = score;
        });

        return activity_scores;
      };

      // Get all activities scored
      // sorted by weight (lower index ==> more precise)
      const activity_type_results = await Promise.all([
        getScrollActivities(),
        with_survey_answers ? getRatingActivities() : Promise.resolve({}),
      ]);


      // Merge the results and keep the score from the latest activity
      const activity_scores = {};
      activity_type_results.forEach((activity_type) => {
        Object.entries(activity_type).forEach(([post_id, score]) => {
          activity_scores[post_id] = score;
        });
      });

      // Remove lowest scores, when over the limit
      if (Object.keys(activity_scores).length > limit) {
        const sorted_scores = Object.entries(activity_scores).sort((a, b) => b[1] - a[1]);
        sorted_scores.slice(limit).forEach(([post_id]) => {
          delete activity_scores[post_id];
        });
      }

      // Fill it with posts by the user (when under min)
      console.time("fillActivities with user posts")
      if (Object.keys(activity_scores).length < min && allow_filling) {
        // Score of authored-post is 3
        const diff = min - Object.keys(activity_scores).length;
        const user_posts = await mongo_hivedb.collection("comments").find({author : username}).project({_id : 1}).limit(diff).toArray();
        user_posts.forEach(({_id}) => {
          activity_scores[_id] = 3.0;
        });
      }
      console.timeEnd("fillActivities with user posts")
      
      // Fill it with votes (when under min)
      console.time("fillActivities with user votes")
      if (Object.keys(activity_scores).length < min && allow_filling) {
        // Vote score is 1.2
        const diff = min - Object.keys(activity_scores).length;
        const user_votes_authorperms = await mongo_hivedb.collection("votes").find({voter : username, weight : {$gt : 100}}).sort({created : -1}).project({author : 1, permlink : 1}).limit(diff).toArray().then(x => x.map(({author, permlink}) => ({author, permlink})));
        const voted_post_ids = await mongo_hivedb.collection("comments").find({$or : user_votes_authorperms}).project({_id : 1}).toArray();
        voted_post_ids.forEach(({_id}) => {
          activity_scores[_id] = 1.2;
        });
      }
      console.timeEnd("fillActivities with user votes")

      return activity_scores;
    }

    async function getAlreadyRecommendedIDs(days_minus = 7, limit = 3000){
        // Build and Execute query
        const startDate = new Date(Date.now() - days_minus * 24 * 60 * 60 * 1000);
        const recommendations = await mongo_client.collection("user_got_recommended").find({username : username}, {metadata : 1, created : 1}).sort({index : 1}).limit(limit).toArray();
        if(recommendations.length == 0) return [];

        // Get all authorperms from the recommendations within the last days_minus days
        const decrypted = await Promise.all(recommendations.map(doc => decryptWorkerPool.exec("decryptMetadata", [prvActivityKey, doc]))).then((result) => result.filter((item) => item));
        const authorperms = decrypted
            .filter(({created}) => created >= startDate)
            .filter(({_id}) => _id?.author && _id?.permlink)
            .map(({_id : metadata}) => ({author : metadata?.author, permlink : metadata?.permlink}));
        if(authorperms.length == 0) return [];

        // Get all post_ids from the recommendations
        const ids = await mongo_hivedb.collection("comments").find({$or : authorperms}, {_id : 1}).toArray().then((docs) => docs.map((doc) => doc._id));
        return ids;
    }

    return {
        getScoredAccountActivities,
        getUserAverageReadingTime,
        getAlreadyRecommendedIDs
    }
  };
};

