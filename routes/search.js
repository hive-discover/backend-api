const queryParser = require('express-query-int');
const bodyParser = require('body-parser')
const express = require('express')
const moment = require('moment');
const axios = require('axios');
const cosinesimil = require('compute-cosine-similarity');

const config = require('../config');
const AVAILABLE_LANGS = ["en", "es"]
const CORRESPONDING_LANG_INDEXES = {
  "en" : "http://hive-comments-last-7d-en.hive-discover.tech",
  "es" : "http://hive-comments-last-7d-es.hive-discover.tech",
}

module.exports = (os_client, mongo_client) => {
  const router = express.Router();
  router.use(bodyParser.json())
  router.use(bodyParser.urlencoded({ extended: true }));
  router.use(queryParser())

  const documents = require('../documents')(os_client, mongo_client);
    
  router.post('/posts', async (req, res) => {
      const start_time = Date.now();
  
      // Required
      const query = req.body.query;
  
      //  * Validate Form
      if(!query || query === "" || typeof query !== "string") {
          res.send({status : "failed", err : "Query is null / not a valid string", code : 1}).end()
          return;
      }
    
      // Optional
      const amount = Math.min(Math.abs(parseInt(req.body.amount || 10)), 100);
      const search_type = req.body.type || "smart";  
      const langs = req.body.langs || AVAILABLE_LANGS;
        
      if(amount > 100)
        return res.send({status : "failed", err : "Amount is too high: should be less than 1k", code : 1}).end()
      if(langs && !Array.isArray(langs))
        return res.send({status : "failed", err : "langs is not an array", code : 1}).end()
      if(["smart", "text", "images"].indexOf(search_type) === -1)
        return res.send({status : "failed", err : "type is not valid", code : 1}).end()   
      if(langs){
        for(const lang of langs){
          if(AVAILABLE_LANGS.indexOf(lang) === -1)
            return res.send({status : "failed", err : "lang is not valid: " + lang, code : 1}).end()
        }
      }
    
      const do_text_search = search_type === "text" || search_type === "smart";
      const do_image_search = search_type === "images" || search_type === "smart";

      // Get word vectors for each lang
      const get_wordvecs = async (tokens, lang) => {
        // Retrieve word vectors from mongo
        const cursor = await mongo_client.db("fasttext").collection("word-vectors-" + lang).find({_id : {$in : tokens}});
        const docs = await cursor.toArray();

        // Build wordvecs object
        let wordvecs = {}; // {word : {idf, vector}}
        for(const doc of docs){
          wordvecs[doc._id] = doc;
        }

        // Convert the vector from binary to float32 array and multiply by idf
        for(const word in wordvecs){
          const idf = wordvecs[word].idf;
          const float_bytes = new Uint8Array(wordvecs[word].vector.buffer);

          wordvecs[word].vector = new Float32Array(float_bytes.buffer);
          wordvecs[word] = wordvecs[word].vector.map(x => x * idf)
        }

        return wordvecs; // {word : {vector weighted by idf}}
      }; 

      const perform_text_search = async () => {
        // Split query into tokens and get word vectors for each lang
        const tokens = query.toLocaleLowerCase().split(" ");
        
        // Get word vectors for each lang
        let wordvecs = {}; // {lang : {word : vector}}
        let wordvecs_promises = langs.map(lang => get_wordvecs(tokens, lang).then(wv => wordvecs[lang] = wv));
        await Promise.all(wordvecs_promises);
        
        // Build quey vector for each lang: sum of word vectors weighted by idf and divided by number of tokens (tf-idf weighted average)
        let query_vectors = {}; // {lang : vector}
        for(const lang in wordvecs){
          let query_vector = new Float32Array(300);
          for(const word in wordvecs[lang]){
            query_vector = query_vector.map((x, i) => x + wordvecs[lang][word][i]);
          }

          query_vectors[lang] = query_vector.map(x => x / tokens.length);
          query_vectors[lang] = Array.from(query_vectors[lang]);
        }   
        
        // Find the most similar documents for each lang
        let search_results = {}; // {lang : {doc_id : score}}
        const search_promises = langs.map(async (lang) => {
          const query = {k : 100, vectors : [query_vectors[lang]]};

          // Call API
          const response = await axios.post(CORRESPONDING_LANG_INDEXES[lang] + "/search", query);
          if(response.status !== 200) throw new Error("Network error while searching for similar vectors");
          if(response.data.status !== "ok") throw new Error("Error while searching for similar vectors");
          if(response.data.results.length === 0) throw new Error("No results found");
          
          // Group results
          const results = response.data.results[0];
          for(const [_id, score] of results){
            if(!search_results[lang]) 
              search_results[lang] = {};

            search_results[lang][_id] = score;
          }
        });
        await Promise.all(search_promises);
        
        // Return the results into a single array and calculate the final score by averaging the scores for each lang
        const final_results = {}; // {doc_id : score}
        for(const lang in search_results){
          for(const doc_id in search_results[lang]){
            if(!final_results[doc_id]) 
              final_results[doc_id] = search_results[lang][doc_id];
            else
              final_results[doc_id] = (final_results[doc_id] + search_results[lang][doc_id]) / 2;
          }
        }

        return final_results;
      };
      
      const perform_image_search = async () => {
        // Encode text with CLIP-API
        const clip_response = await axios.get("https://clip-api.hive-discover.tech/encode-text", {params : {text : query}});
        if(clip_response.status !== 200) throw new Error("Network error while encoding text");
        if(!Array.isArray(clip_response.data)) throw new Error("Error while encoding text");
        if(clip_response.data.length === 0) throw new Error("No results found");
        if(clip_response.data.length !== 512) throw new Error("Results are not 512-dimensional");

        // Build query and send it to the image search API
        const knn_query = {k : 100, vectors : [clip_response.data]};
        const knn_response = await axios.post("http://hive-comments-last-7d-images.hive-discover.tech/search", knn_query);
        if(knn_response.status !== 200) throw new Error("Network error while searching for similar image vectors");
        if(knn_response.data.status !== "ok") throw new Error("Error while searching for similar image vectors");
        if(knn_response.data.results.length === 0) throw new Error("No results found");

        // Get all image_urls remapped
        const knn_results =  Object.fromEntries(knn_response.data.results[0]); // [[url, score], ...] ==> {url : score}
        
        // Possible approaches:
        //  - Get the comments for each image and add the scores together per image in comment. High possibility that more than one image occurs in a comment (faster, but less accurate) [current]
        //  - Get the comments and retrieve all other images of the same post as well. Compare them with the query and calculate a total score for each post (slower, but more accurate) [could be implemented in the future]

        // Get comment ids and images
        const comment_with_images = {}; // {comment_id : [image_url, ...]}
        const cursor = mongo_client.db("hive").collection("comments").find({"json_metadata.image" : {$in : Object.keys(knn_results)}}, {projection : {_id : 1, "json_metadata.image" : 1}});
        for await(const doc of cursor){
          comment_with_images[doc._id] = doc.json_metadata.image;
        }

        // Calculate the final scores for each post by averaging all image-scores together, when the image was returned by the knn-search
        const final_results = {}; // {doc_id : score}
        for(const [comment_id, images] of Object.entries(comment_with_images)){
          final_results[comment_id] = 0;

          for(const image of images){
            if(!knn_results[image]) 
              continue;
            
            final_results[comment_id] += (knn_results[image] / images.length);
          }
        } 
      
        return final_results;
      };

      const search_catcher = (err) => {
        console.error(err);
        return res.status(500).json({status : "failed", code : 5});
      }

      // Perform searches
      const text_search_task = do_text_search ? perform_text_search().catch(search_catcher) : new Promise((resolve) => resolve({}));  
      const image_search_task = do_image_search ? perform_image_search().catch(search_catcher) : new Promise((resolve) => resolve({}));

      // Wait for all searches to complete
      // weight text search with 2/3 and image search with 1/3
      let [text_search_results, image_search_results] = await Promise.all([text_search_task, image_search_task]);
      if(!text_search_results || !image_search_results) 
        return res.status(400).json({status : "failed", code : 4});

      text_search_results =Object.entries(text_search_results).filter(([k, v,]) => v).map(([k, v]) => [k, v * 2/3]);
      image_search_results = Object.entries(image_search_results).filter(([k, v,]) => v).map(([k, v]) => [k, v * 1/3]);
      const combined_results = [...text_search_results, ...image_search_results]; // [[doc_id, score], ...]

      // Merge results by dividing with the highest possible score
      const highest_score = do_text_search && do_image_search ? 2 : (do_text_search ? 4/3 : 2/3)
      const final_results = {}; // {doc_id : score}
      for(const [doc_id, score] of combined_results){    
          if(!final_results[doc_id]) 
            final_results[doc_id] = 0;

          final_results[doc_id] += score / highest_score;
      }

      // Sort results by score and limit
      const results = Object.entries(final_results).sort((a, b) => b[1] - a[1]).slice(0, amount);

      // Get authorperms
      const authorperms = await mongo_client.db("hive").collection("comments").find({_id : {$in : results.map(x => parseInt(x[0]))}}).project({author : 1, permlink : 1}).toArray();
      const authorperm_map = {}; // {doc_id : {author, perm}}
      authorperms.forEach(x => authorperm_map[x._id] = {author : x.author, permlink : x.permlink});

      // Replace the post-id with the authorperm and add the score
      results.forEach((x, i) => {
        const [doc_id, score] = x;
        const authorperm = authorperm_map[doc_id];

        if(!authorperm) 
          return null;
        results[i] = {author : authorperm.author, permlink : authorperm.permlink, score : score, id : parseInt(doc_id)};
      });

      const elapsedSeconds = (Date.now() - start_time) / 1000;
      res.send({status : "ok", posts : results.filter(x => x), time : elapsedSeconds}).end();
  });
    
  router.post('/similar-post', async (req, res) => {
    const start_time = Date.now();
  
    // Required
    const author = req.body.author;
    const permlink = req.body.permlink;
    
    if(!author||!permlink) {
        res.send({status : "failed", err : "Query is null", code : 1}).end()
        return;
    }
  
    // Optional
    const amount = Math.min(parseInt(req.body.amount || 7), 50);
    const tags = req.body.tags || [];
    const parent_permlinks = req.body.parent_permlinks || [];
    const wanted_langs = req.body.langs || AVAILABLE_LANGS;
  
    //  * Validate form
    if(!Array.isArray(parent_permlinks)){
      res.send({status : "failed", err : "parent_permlinks is not an array", code : 1}).end()
      return;
    }
    if(!Array.isArray(tags)){
      res.send({status : "failed", err : "tags is not an array", code : 1}).end()
      return;
    }
    if(!Array.isArray(wanted_langs)){
      res.send({status : "failed", err : "langs is not an array", code : 1}).end()
      return;
    }
    if(wanted_langs){
      for(const lang of wanted_langs){
        if(AVAILABLE_LANGS.indexOf(lang) === -1)
          return res.send({status : "failed", err : "lang is not valid: " + lang, code : 1}).end()
      }
    }
  
    // Get the source post
    const source_post = await mongo_client.db("hive").collection("comments").findOne({author, permlink}, {projection : {doc_vectors : 1, avg_image_vector : 1}});
    if(!source_post)
       return res.send({status : "failed", err : "Source post not found", code : 1}).end()

    const search_promises = [];

    // Lang searches
    if(source_post.doc_vectors){
      if(source_post.doc_vectors.en && wanted_langs.includes("en"))
        search_promises.push(documents.findSimilarByVectors([source_post.doc_vectors.en], 100, "en"));
      if(source_post.doc_vectors.es && wanted_langs.includes("es"))
        search_promises.push(documents.findSimilarByVectors([source_post.doc_vectors.es], 100, "es"));
    }

    // Image search
    if(source_post.avg_image_vector)
      search_promises.push(documents.findSimilarByVectors([source_post.avg_image_vector], 100, "avg_image_vector"));

    // Wait for all searches to finish
    const similar_results = await Promise.all(search_promises).then(res => res.flat())
                                    .then(res => res.map(x => Object.entries(x).map(([k,v]) => ({_id : k, score : v}))))
                                    .then(res => res.flat())

    // Combine results to a single object
    const post_scores = {}; // {_id : score}   
    for(const result of similar_results){
      if(!post_scores[result._id])
        post_scores[result._id] = 0;

      post_scores[result._id] += result.score / search_promises.length;
    }

    // Remove source post
    delete post_scores[source_post._id];

    // Build mongo query
    if(tags.length > 0 || parent_permlinks.length > 0){
      const mongo_query = {
        _id : {$in : Object.keys(post_scores).map(x => parseInt(x))}
      }
      if(tags.length > 0)
        mongo_query["json_metadata.tags"] = {$nin : tags}
      if(parent_permlinks.length > 0)
        mongo_query.parent_permlink = {$nin : parent_permlinks};

      // Remove returned posts
      const filtered_posts = await mongo_client.db("hive").collection("comments").find(mongo_query, {projection : {_id : 1}}).toArray();
      for(const post of filtered_posts)
        delete post_scores[post._id];
    }

    // Sort by score and return top amount
    const sorted_post_scores = Object.entries(post_scores).sort((a, b) => b[1] - a[1]).slice(0, amount);
    
    // Get author, permlink from db of sorted_post_scores
    const post_ids = await mongo_client.db("hive").collection("comments").find({_id : {$in : sorted_post_scores.map(item => parseInt(item[0]))}}, {projection : {author : 1, permlink : 1}}).toArray();

    // Build response
    const result = sorted_post_scores.map((item, index) => {
      const post = post_ids.find(post => post._id == item[0]);
      return {
        author : post.author,
        permlink : post.permlink,
        score : item[1] / 2
      }
    });


    // Send response
    const elapsedSeconds = (Date.now() - start_time) / 1000;
    res.send({status : "ok", posts : result, time : elapsedSeconds}).end();
  })
  
  router.post('/similar-by-author', async (req, res) => { 
    const start_time = Date.now();
  
    // Required
    const author = req.body.author;
    const permlink = req.body.permlink;
    
    if(!author||!permlink) {
        res.send({status : "failed", err : "Query is null", code : 1}).end()
        return;
    }

    // Optional
    const timerange = req.body.timerange || "full";
    const amount = Math.min(parseInt(req.body.amount || 7), 50);
  
    if(!["full", "month", "week", "day"].includes(timerange))
      return res.send({status : "failed", err : "timerange is not valid. Use one of: full, month, week, day", code : 1}).end()
    
    // get source post
    const source_post = await mongo_client.db("hive").collection("comments").findOne({author, permlink}, {projection : {doc_vectors : 1, avg_image_vector : 1}});
    if(!source_post)
        return res.send({status : "failed", err : "Source post not found", code : 1}).end()

    const parseFloatBinary = (bin) => {
      const float_bytes = new Uint8Array(bin.buffer);
      return Array.from(new Float32Array(float_bytes.buffer));
    }

    if(source_post.doc_vectors){
      if(source_post.doc_vectors.en)
        source_post.doc_vectors.en = parseFloatBinary(source_post.doc_vectors.en);
      if(source_post.doc_vectors.es)
        source_post.doc_vectors.es = parseFloatBinary(source_post.doc_vectors.es);
    }

    if(source_post.avg_image_vector)
      source_post.avg_image_vector = parseFloatBinary(source_post.avg_image_vector);


    // Get all posts by the author in the timerange
    const timerange_dates = {
      "full" : new Date(0),
      "month" : new Date(Date.now() - 30 * 24 * 60 * 60 * 1000),
      "week" : new Date(Date.now() - 7 * 24 * 60 * 60 * 1000),
      "day" : new Date(Date.now() - 24 * 60 * 60 * 1000)
    };
    let posts_by_author = await mongo_client.db("hive").collection("comments").find({author, created : {$gt : timerange_dates[timerange]}, _id : {$ne : source_post._id}}, {projection : {_id : 1, doc_vectors : 1, avg_image_vector : 1, author: 1, permlink : 1}}).toArray();

    // Parse all binary vectors to float32 arrays
    posts_by_author = posts_by_author.map(post => {
      if(post.doc_vectors){
        if(post.doc_vectors.en)
          post.doc_vectors.en = parseFloatBinary(post.doc_vectors.en);
        if(post.doc_vectors.es)
          post.doc_vectors.es = parseFloatBinary(post.doc_vectors.es);
      }
      if(post.avg_image_vector)
        post.avg_image_vector = parseFloatBinary(post.avg_image_vector);

      return post;
    });

    // Calculate similarity
    const similar_posts = posts_by_author.map(post => {
      let score = 0;

      if(source_post.doc_vectors && post.doc_vectors){
        if(source_post.doc_vectors.en && post.doc_vectors.en)
          score += cosinesimil(source_post.doc_vectors.en, post.doc_vectors.en) + 1;
        if(source_post.doc_vectors.es && post.doc_vectors.es)
          score += cosinesimil(source_post.doc_vectors.es, post.doc_vectors.es) + 1;

        if(source_post.doc_vectors.en && post.doc_vectors.es)
          score /= 2;
      }

      if(source_post.avg_image_vector && post.avg_image_vector)
        score += cosinesimil(source_post.avg_image_vector, post.avg_image_vector) + 1;

      return {
        _id : post._id,
        author : post.author,
        permlink : post.permlink,
        score : score / 4
      }
    }).sort((a, b) => b.score - a.score).slice(0, amount);
  
    // Send response
    const elapsedSeconds = (Date.now() - start_time) / 1000;
    res.send({status : "ok", posts : similar_posts, time : elapsedSeconds}).end();
  })

  return router;
}