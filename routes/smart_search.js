const queryParser = require('express-query-int');
const bodyParser = require('body-parser')
const express = require('express')
const moment = require('moment');
const request = require('request');
const Base58 = require('bs58');

const config = require('../config');

module.exports = (os_client, mongo_client) => {
  const router = express.Router();
  router.use(bodyParser.json())
  router.use(bodyParser.urlencoded({ extended: true }));
  router.use(queryParser())
    
    MAX_ITEMS = 250;

    async function textSearch(pre_filters, text_query, highlight){
        const search_query = {    
            "size" : MAX_ITEMS,
            "from" : 0,
            "query" : {
                "bool" : {
                  "must" : [
                    ...pre_filters, 
                    {
                      "multi_match" : {
                        "query" : text_query,
                        "type": "phrase",
                        "fields" : ["text_title^3", "text_body"]
                      }
                    }
                  ]
                }
            },
            "highlight" : highlight,
            "sort" : [{ _score : { order: "desc" } }],    
            "_source" : {"includes" : ["author", "permlink"]}   
        };

        // Do the search
        const os_resp = await os_client.search({index:"hive-posts", body : search_query})
        if(os_resp.statusCode !== 200 || os_resp?.error)
            throw new Error("Error while searching for text");

        // Remap the results: {author, permlink, score, highlight}
        return os_resp.body.hits.hits.map((hit) => {
            return {
                _id : hit._id,
                author : hit._source.author,
                permlink : hit._source.permlink,
                score : hit._score,
                highlight : hit.highlight
            }
        });

    }

    async function imageSearch(pre_filters, text){
        const getTextEmbedding = async(fail_counter = 0) => {
            const {response, body, error} = await new Promise((resolve) => {
              request({url : config.CLIP_API_ADDRESS + "/encode-text", qs : {text : text}, method : "GET"}, (error, response, body ) => resolve({response, body, error}));
            });
    
            if(response.statusCode !== 200 || error){
              if(fail_counter > 5) 
                return null;
    
              // Sleep 200ms and try again
              await new Promise(resolve => setTimeout(resolve, 200));
              return await getTextEmbedding(fail_counter + 1);
            }
    
            // Return the embedding as array
            return {embedding : JSON.parse(body), fail_counter};
          }

        const {embedding : embedded_text, fail_counter : emb_fail_counter} = await getTextEmbedding();
        if(!embedded_text)
            throw new Error(`Failed to get text embedding (${emb_fail_counter} times)`);

        const search_query = {
            "size" : MAX_ITEMS,
            "query" : {
              "script_score": {
                "query": {bool : {filter : [{bool : {must : pre_filters}}]}},
                "script": {
                  "source": "knn_score",
                  "lang": "knn",
                  "params": {
                    "field": "avg_clip_vector",
                    "query_value": embedded_text,
                    "space_type": "cosinesimil"
                  }
                }
              }     
            },
            "_source" : {
              "includes" : ["author", "permlink"]
            }    
          };
        
          // Do the search
        const os_resp = await os_client.search({index:"hive-posts", body : search_query})
        if(os_resp.statusCode !== 200 || os_resp?.error)
            throw new Error("Error while searching for images");

        // Remap the results: {author, permlink, score, image}
        return os_resp.body.hits.hits.map((hit) => {
            return {
                _id : hit._id,
                author : hit._source.author,
                permlink : hit._source.permlink,
                score : hit._score,
                image : hit._source.image
            }
        });
    }

  router.post('/', async (req, res) => {
      const start_time = Date.now();
  
      // Required
      const query = req.body.query;
  
      //  * Validate Form
      if(!query) {
          res.send({status : "failed", err : "Query is null", code : 1}).end()
          return;
      }
    
      // Optional
      const amount = Math.min(Math.abs(parseInt(req.body.amount || 10)), 100);
      const page_number = Math.max(Math.abs(parseInt(req.body.page_number || 1)), 1);
  
      const highlight = req.body.highlight || false;
  
      const tags = req.body.tags || [];
      const authors = req.body.authors || [];
      const parent_permlinks = req.body.parent_permlinks || [];
      const min_votes = parseInt(req.body.min_votes || 0);
      const max_votes = parseInt(req.body.max_votes || 0);
      const wanted_langs = req.body.langs || [];
      const start_date = req.body.start_date || null;
      const end_date = req.body.end_date || null;
    
      //  * Validate Form
      if((amount * page_number) > MAX_ITEMS) {
          res.send({status : "failed", err : `The result length is limited to ${MAX_ITEMS} items. So, amount * page_number has to be lower than this!`, code : 1}).end()
          return;
      }
      if(!Array.isArray(tags)){
        res.send({status : "failed", err : "tags is not an array", code : 1}).end()
        return;
      }
      if(!Array.isArray(authors)){
        res.send({status : "failed", err : "authors is not an array", code : 1}).end()
        return;
      }
      if(!Array.isArray(parent_permlinks)){
        res.send({status : "failed", err : "parent_permlinks is not an array", code : 1}).end()
        return;
      }
      if(!Array.isArray(wanted_langs)){
        res.send({status : "failed", err : "langs is not an array", code : 1}).end()
        return;
      }
      if(start_date && !moment(start_date, "YYYY-MM-DD", true).isValid()){
        res.send({status : "failed", err : "start_date is not formatted correctly: 'YYYY-MM-DD'", code : 1}).end()
        return;
      }
      if(end_date && !moment(end_date, "YYYY-MM-DD", true).isValid()){
        res.send({status : "failed", err : "end_date is not formatted correctly: 'YYYY-MM-DD'", code : 1}).end()
        return;
      }
  
      let additional_information = []
  
      // Build pre filter query
      const pre_filter_query = [];
  
        // Set tags, parent_permlink and author terms-filter
        if(parent_permlinks.length > 0)
            pre_filter_query.push({"terms" : {"parent_permlink" : parent_permlinks}})
        if(tags.length > 0)
            pre_filter_query.push({"terms" : {"tags" : tags}})
        if(authors.length > 0)
            pre_filter_query.push({"terms" : {"author" : authors}})

        // Set start_date and end_date range-filter
        if(start_date && !end_date)
            pre_filter_query.push({"range" : {"timestamp" : {"gte" : start_date}}});
        else if(!start_date && end_date)
            pre_filter_query.push({"range" : {"timestamp" : {"lte" : end_date}}});
        else if(start_date && end_date)
            pre_filter_query.push({"range" : {"timestamp" : {"lte" : end_date, "gte" : start_date}}});
  
        // Set wanted_langs nested-terms-filter
        if(wanted_langs.length > 0){
            pre_filter_query.push({
            "nested" : {
              "path" : "language",
              "query" : {
                "bool" : {
                  "must" : [
                    {
                      "terms" : {
                        "language.lang" : wanted_langs
                      }
                    },
                    {
                      "range" : {
                        "language.x" : {"gte" : 0.5}
                      }
                    }
                  ]
                }
              }
            }
          })
        }
  
        // Set min_votes and max_votes range-filter (most performance heavy)
        if(min_votes > 0 && max_votes === 0)
            pre_filter_query.push({ "script": { "script": {  "source":"doc['votes'].length >= " + min_votes}}});
        else if(min_votes === 0 && max_votes > 0)
            pre_filter_query.push({ "script": { "script": {  "source":"doc['votes'].length <= " + max_votes}}});
        else if(min_votes > 0 && max_votes > 0)
            pre_filter_query.push({ "script": { "script": {  "source":"doc['votes'].length >= " + min_votes + " && doc['votes'].length <= " + max_votes}}});
  
      const getHighlight = () => {
        if(highlight){
          return {
              pre_tags : ["<mark>"],
              post_tags : ["</mark>"],
              fields : {
                "text_body" : {},
                "text_title" : {}
              }
          }
        } else {
          return {};
        }
      
      }
      
  
     const [text_results, image_results] = await Promise.all([
        textSearch(pre_filter_query, query, getHighlight()),
        imageSearch(pre_filter_query, query)
    ]).catch(err => {
        console.error("Error on Route/smart_search/: ", err);       
        return [null, null]
    });

    if(!text_results || !image_results)
        return res.send({status : "failed", err : "Search failed unexpected", code : 2}).end();
  
    // Merge image-search results into text-search results by multiplying the scores
    const merged_results = text_results.map((text_res_item) => {
        // Find this result in the image-results
        const image_result = image_results.find(img_res_item => img_res_item._id === text_res_item._id);
        if(!image_result)
            return text_res_item;

        text_res_item._score *= image_result._score;
        return text_res_item;
    });

    // Sort merged results by score
    merged_results.sort((a, b) => b._score - a._score);
        
    // Get the results for the current page
    const results = merged_results.slice(amount * (page_number - 1), amount * page_number);

      const elapsedSeconds = (Date.now() - start_time) / 1000;
      res.send({status : "ok", posts : results, additional_information : additional_information, time : elapsedSeconds}).end();
  });



  return router;
}