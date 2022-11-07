const similarity = require( 'compute-cosine-similarity' );
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

// function calcSimilarPostScores(similar_posts, sample_doc_vectors) {
//     return similar_posts.map(post => {
//         const post_id = config.getCommentID(post);
  
//         // Calculate cosine-similarity
//         // (average calculation can be ignored because it is everytime the same divisor: sample_batch_size)
//         let total_sims = [];
//         Object.keys(post?.doc_vector || {}).forEach(lang => {
//           total_sims.push(
//             Object.values(sample_doc_vectors).map(doc_item => {
//               // Calculate sample-similar-cosine-similarity for this language
//               if(!doc_item[lang] || !post.doc_vector[lang]) return 0;
  
//               // TODO: select if this similarity is good or bad
//               return similarity(doc_item[lang], post.doc_vector[lang]) + 1;
//             }).reduce((a, b) => a + b, 0)
//           ); 
//         });
      
//         return total_sims.map(sim => [post_id, sim]);
//       }).flat()
//         .filter(item => item[1] > 0)
//         // Remove duplicated posts and keep the item with the highest score
//         .reduce((acc, item) => {
//           acc[item[0]] = (acc[item[0]] || 0) < item[1] ? item[1] : acc[item[0]];
//           return acc;
//         }, {});
// }

module.exports = (os_client, mongo_client) => {

    const documents = require('./documents')(os_client, mongo_client);
    const getUser = require('./users')(os_client, mongo_client);

  let startTime;
    const getFeed = async (username, prvActivityKey, amount, filter) => {
        startTime = performance.now();
        const user = await getUser(username, prvActivityKey);
        console.log(`getUser - ${performance.now() - startTime} ms`);

        // 0. Start tasks in background to have the result later prepared
        const already_recommended_ids = user.getAlreadyRecommendedIDs(username);

        // 1. Get Scored Users Activitys
        startTime = performance.now();
        const activity_scores = await user.getScoredAccountActivities(1000, 50, true);
        console.log(`getUserActivity - ${performance.now() - startTime} ms`);

        // 2. Get a random sample of activities (weighted by the interest-score) and keep the original score
        startTime = performance.now();
        const sample_items = sampleRandomWeighted(Object.entries(activity_scores), 25).map(v => [v[0], activity_scores[v[0]]]);
        const sample_ids = sample_items.map(v => v[0]);
        console.log(`getRandomSample of activities - ${performance.now() - startTime} ms`);

        const filter_out_ids = [...(await already_recommended_ids), ...Object.keys(activity_scores)];

        // 3. Find similar posts to the sample
        startTime = performance.now();
        const similar_post_ids = await documents.findSimilarPosts(
          sample_ids, 
          4 + (filter?.distraction || 0),
          username,
          filter.tags,
          filter.parent_permlinks,
          filter_out_ids
        );
        console.log(`getSimilarPosts - ${performance.now() - startTime} ms`);
        
        // 4. Calculate the similarity score for each similar post to the sample and sum it up
        startTime = performance.now();
        const similar_post_scores = Object.fromEntries(
          await Promise.all(
            similar_post_ids.map(async (id) => {
              const result = await documents.calcSimilarScores(id, sample_ids); // [score to sample_ids[0], score to sample_ids[1], ...]
              return [id, result.reduce((a, b) => a + b, 0)];
            })
          ) 
        );// {post_id : total_sim_score, ...}
        console.log(`calcSimilarPostScoresTotal - ${performance.now() - startTime} ms`);
        
        // 5. Get the final amount of posts (randomly weighted by the total-sim-score score)
        startTime = performance.now();
        const selected_ids = sampleRandomWeighted(Object.entries(similar_post_scores), amount).map(v => v[0]);
        console.log(`getRandomSample of Similar - ${performance.now() - startTime} ms`);
        
        // 6. Get the final author / permlinks
        startTime = performance.now();
        const selected_posts = await documents.getAuthorPermlinks(selected_ids);
        console.log(`getSelectedPosts - ${performance.now() - startTime} ms`);
        

        return selected_posts
        // 3. Get the doc-vectors of the sample
        // console.time("getSampleDocVectors")
        // const sample_doc_vectors = await documents.getDocVectors(sample_ids.map(v => v[0]));
        // console.timeEnd("getSampleDocVectors")

        // // 3.1 Filter not-wanted languages (if filter.langs is set)
        // if (filter?.langs && filter.langs.length > 0) {
        //     Object.keys(sample_doc_vectors).forEach(id => {
        //         Object.keys(sample_doc_vectors[id]).forEach(lang => {
        //             if (!filter.langs.includes(lang))
        //                 delete sample_doc_vectors[id][lang];
        //         });
        //     });
        // }

        // // 4. Select the k most similar vectors
        // console.time("getSimilarPosts")
        // const filter_out_ids = [...(await already_recommended_ids), ...Object.keys(activity_scores)];
        // const similar_posts = await documents.findSimilarToDocVectors(
        //     sample_doc_vectors,
        //     4 + (filter?.distraction || 0),
        //     username,
        //     filter.tags,
        //     filter.parent_permlinks,
        //     filter_out_ids // filter out already seen or recommended posts
        // ).then(x => x.flat());
        // console.timeEnd("getSimilarPosts")

        // // 5. calculate the score of the similar posts with the total cosine similarity score with the sample-ones
        // console.time("calcSimilarPostScores")
        // const similar_posts_scores = calcSimilarPostScores(similar_posts, sample_doc_vectors);
        // console.timeEnd("calcSimilarPostScores")

        // // 6. select the right amount of posts randomly weighted by the score
        // console.time("getRandomSelection")
        // const selected_ids = sampleRandomWeighted(Object.entries(similar_posts_scores), amount).map(v => v[0]);
        // console.timeEnd("getRandomSelection")

        // // 7. get the authorperms of the selected posts
        // console.time("getSelectedPosts")
        // const selected_posts = await documents.getAuthorPermlinks(selected_ids);
        // console.timeEnd("getSelectedPosts")

        // return selected_posts;
    }

    return {getFeed};
}