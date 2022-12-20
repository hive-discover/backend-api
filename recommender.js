const config = require('./config');
const { performance } = require("perf_hooks");
const userAuth = require('./user_auth');

function sampleRandomWeighted(weighted_ids, n = 0) {
    // Randomly sort weighted
    // StackOverflow Answer to a question: https://stackoverflow.com/a/65207342/7586306
    //  ==> Perform Exponential Distribution

    // weighted_ids = [id, weight]
    weighted_ids = weighted_ids.map(v => [v[0], Math.log10(1 - Math.random()) / v[1]]);
    // lowest is to choose
    weighted_ids.sort((a, b) => (a[1] < b[1]) ? -1 : 1);

    if (n > 0)
        return weighted_ids.slice(0, n);
    return weighted_ids;
}

module.exports = (os_client, mongo_client) => {

    const documents = require('./documents')(os_client, mongo_client);
    const getUser = require('./users')(os_client, mongo_client);

    const filterIDs = async (filter, ids) => {
      // Build the query
      const query = {_id : {$in : ids}};
      if(filter){
        if(filter?.parent_permlinks){
          query["parent_permlink"] = {$in : filter.parent_permlinks};
        }
        if(filter?.tags){
          query["json_metadata.tags"] = {$in : filter.authors};
        }
        if(filter?.langs){
          filter.langs = filter.langs.map(lang => ({["known_tokens_ratio." + lang] : {$gt : 0.3}}));
          query["$or"] = filter.langs;
        }
      }

      // Get the filtered ids
      const filtered_ids = await mongo_client.db("hive").collection("comments").find(query).project({_id : 1}).toArray();
      return filtered_ids.map(v => v._id);
    }

    let startTime;
    const getFeed = async (username, prvActivityKey, amount, filter) => {
        startTime = performance.now();
        const user = await getUser(username, prvActivityKey);
        console.log(`getUser - ${performance.now() - startTime} ms`);

        // 0. Start tasks in background to have the result later prepared
        const already_recommended_ids = user.getAlreadyRecommendedIDs();

        // 1. Get Scored Users Activitys
        startTime = performance.now();
        const min_activity_count = Object.keys(filter).length > 0 ? 150 : 30;
        const activity_scores = await user.getScoredAccountActivities(1000, min_activity_count, true);
        console.log(`getUserActivity - ${performance.now() - startTime} ms`);

        // 2. Get a random sample of activities (weighted by the interest-score) and keep the original score
        startTime = performance.now();
        const sample_items = sampleRandomWeighted(Object.entries(activity_scores), 35).map(v => [parseInt(v[0]), activity_scores[v[0]]]);
        const sample_ids = sample_items.map(v => v[0]);
        console.log(`getRandomSample of activities - ${performance.now() - startTime} ms`);

        // 3. Get all available vectors for the sample
        startTime = performance.now();
        const sample_vectors = await mongo_client.db("hive").collection("comments").find({ _id: { $in: sample_ids } }).project({ _id: 1, doc_vectors: 1, avg_image_vector : 1 }).toArray();
        console.log(`getAllVectors for sample - ${performance.now() - startTime} ms`);

        // 4. Find all similar items for the sample
        startTime = performance.now();
        const k = Object.keys(filter).length > 0 ? 50 : 12;
        const similar_results = await Promise.all([
          documents.findSimilarByVectors(sample_vectors.filter(v => v?.doc_vectors?.en).map(v => v.doc_vectors.en), k, "en"),
          documents.findSimilarByVectors(sample_vectors.filter(v => v?.doc_vectors?.es).map(v => v.doc_vectors.es), k, "es"),
          documents.findSimilarByVectors(sample_vectors.filter(v => v?.avg_image_vector).map(v => v.avg_image_vector), k, "avg_image_vector"),
        ]).catch(e => console.error(e));
        console.log(`findSimilarItems for sample - ${performance.now() - startTime} ms`);

        if (!similar_results) return [];

        // 5. Calculate the total sim score
        // With 3 different similar-apis, we got a max-score of 6 (n * 2)
        startTime = performance.now();
        const similar_items_scores = {}; // { id: total score }
        for(const type_result of similar_results)
        {
          for(const item_results of type_result)
          {
            for(const [id, score] of Object.entries(item_results))
            {
              if(!similar_items_scores[id]) 
                similar_items_scores[id] = 0;
              similar_items_scores[id] += score / 6;
            }
          }
        }
        console.log(`calculateSimilarItemsScores - ${performance.now() - startTime} ms`);

        // 6. Filter out all not-wanted items
        startTime = performance.now();
        const filter_out_ids = [...(await already_recommended_ids), ...Object.keys(activity_scores)].map(v => v.toString());
        let filtered_items = Object.entries(similar_items_scores).filter(([id, score]) => !filter_out_ids.includes(id.toString()));
        console.log(`filterOutNotWantedItems - ${performance.now() - startTime} ms`);

        // 7. Filter IDs
        startTime = performance.now();
        const filtered_ids = await filterIDs(filter, filtered_items.map(v => parseInt(v[0])));
        filtered_items = filtered_items.filter(([id, score]) => filtered_ids.includes(parseInt(id)));
        console.log(`filterIDs - ${performance.now() - startTime} ms`);


        // 8. Get the final posts by selecting randomly weighted by the score
        startTime = performance.now();
        const final_items = sampleRandomWeighted(filtered_items, amount);
        console.log(`getRandomSample of finalItems - ${performance.now() - startTime} ms`);

        // 9. get the author permlinks
        startTime = performance.now();
        const final_items_posts = await mongo_client.db("hive").collection("comments").find({ _id: { $in: final_items.map(v => parseInt(v[0])) } }).project({ _id: 1, author: 1, permlink: 1 }).toArray();
        console.log(`getAuthorPermlinks - ${performance.now() - startTime} ms`);

        return final_items_posts.map(v => ({ author: v.author, permlink: v.permlink }));
    }

    return {getFeed};
}