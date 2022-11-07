const queryParser = require('express-query-int');
const bodyParser = require('body-parser')
const express = require('express')
const moment = require('moment');

const config = require('../config');

module.exports = (os_client, mongo_client) => {
  const router = express.Router();
  router.use(bodyParser.json())
  router.use(bodyParser.urlencoded({ extended: true }));
  router.use(queryParser())
    
  router.post('/posts', async (req, res) => {
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
      let sort_mode = req.body.sort_mode || "smart";
  
      const tags = req.body.tags || [];
      const authors = req.body.authors || [];
      const parent_permlinks = req.body.parent_permlinks || [];
      const min_votes = parseInt(req.body.min_votes || 0);
      const max_votes = parseInt(req.body.max_votes || 0);
      const wanted_langs = req.body.langs || [];
      const start_date = req.body.start_date || null;
      const end_date = req.body.end_date || null;
    
      //  * Validate Form
      if((amount * page_number) > 10000) {
          res.send({status : "failed", err : "The result length is limited to 10,000 items. So, amount * page_number has to be lower than 10,000!", code : 1}).end()
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
  
      // Build Search Query
      const getOtherFilters = () => {
        let query = [];
  
        // Set tags, parent_permlink and author terms-filter
        if(parent_permlinks.length > 0)
          query.push({"terms" : {"parent_permlink" : parent_permlinks}})
        if(tags.length > 0)
          query.push({"terms" : {"tags" : tags}})
        if(authors.length > 0)
          query.push({"terms" : {"author" : authors}})
  
        // Set start_date and end_date range-filter
        if(start_date && !end_date)
          query.push({"range" : {"timestamp" : {"gte" : start_date}}});
        else if(!start_date && end_date)
          query.push({"range" : {"timestamp" : {"lte" : end_date}}});
        else if(start_date && end_date)
          query.push({"range" : {"timestamp" : {"lte" : end_date, "gte" : start_date}}});
  
        // Set wanted_langs nested-terms-filter
        if(wanted_langs.length > 0){
          query.push({
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
          query.push({ "script": { "script": {  "source":"doc['votes'].length >= " + min_votes}}});
        else if(min_votes === 0 && max_votes > 0)
          query.push({ "script": { "script": {  "source":"doc['votes'].length <= " + max_votes}}});
        else if(min_votes > 0 && max_votes > 0)
          query.push({ "script": { "script": {  "source":"doc['votes'].length >= " + min_votes + " && doc['votes'].length <= " + max_votes}}});
  
        return query;
      }
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
      const getSortMode = () => {
        switch(sort_mode){
          case "latest":
            return [{timestamp : {"order" : "desc"}}];
          case "oldest":
            return [{timestamp : {"order" : "asc"}}];
          case "relevance":
            return [{ _score : { order: "desc" } }];
          case "votes":
            return { "_script": { "script": "doc.upvotes.size()", "order": "desc", "type" : "number" }};
          case "smart" :
            return [{ _score : { order: "desc" } }, {timestamp : {"order" : "desc"}}, { "_script": { "script": "doc.upvotes.size()", "order": "desc", "type" : "number" }}];
          default:
            // Default == relevance
            additional_information.push("Unknown Sort Mode (got: '" + sort_mode + "')! Automatically changed to 'smart'");
            sort_mode = "smart";
            return getSortMode();
        }
      }
  
      const search_query = {    
          "size" : amount,
          "from" : (page_number - 1) * amount,
          "query" : {
              "bool" : {
                "must" : [
                  ...getOtherFilters(), 
                  {
                    "multi_match" : {
                      "query" : query,
                      "type": "phrase",
                      "fields" : ["text_title^2", "text_body"]
                    }
                  }
                ]
              }
          },
          "highlight" : getHighlight(),
          "sort" : getSortMode(),    
          "_source" : {"includes" : ["author", "permlink"]}   
      };
  
      // Make this search request
      const [os_search_response, success] = await 
          os_client.search({index:"hive-posts", body : search_query})
          .then(response => [response, response.statusCode === 200])
          .catch(err => {console.error("Error on Route/Search/Post: ", err); return [null, false]});
  
      if(!success)
          return res.send({status : "failed", err : "Search request failed enexpected", code : 0}).end();
  
      // Remap Results and send response
      let results = os_search_response.body.hits.hits;
      results = results.map(item => {return {author : item._source.author, permlink : item._source.permlink, score : item._score, highlight : item.highlight}});
  
      const total_matched_docs = os_search_response.body.hits.total.value;
      const elapsedSeconds = (Date.now() - start_time) / 1000;
      res.send({status : "ok", posts : results, additional_information : additional_information, time : elapsedSeconds, sort_mode : sort_mode, total_matched_docs : total_matched_docs}).end();
  });
  
  router.post('/accounts', async (req, res) => {
    const start_time = Date.now();
  
    // Required
    const query = req.body.query;
    
    //  * Validate Required Form
    if(!query)
      return res.send({status : "failed", err : "Query is null", code : 1}).end()
  
    // Optional
    const amount = Math.min(Math.abs(parseInt(req.body.amount || 10)), 100);
    const page_number = Math.max(Math.abs(parseInt(req.body.page_number || 1)), 1);
  
    //  * Validate Optional Form
    if((amount * page_number) > 10000) {
      res.send({status : "failed", err : "The result length is limited to 10,000 items. So, amount * page_number has to be lower than 10,000!", code : 1}).end()
      return;
    }
  
    const search_query = {
      "size" : amount,
      "from" : (page_number - 1) * amount,
      "query" : {
        "bool" : {
          "should" : [
            {
              "match": {
                "name": {
                  "query": query,
                  "analyzer": "standard"
                }
              }
            },
            {
              "nested" : {
                "path": "profile",
                "query" : {
                  "multi_match": {
                    "query": query,
                    "fields": ["profile.name^4", "profile.about^2", "profile.location"]
                  }
                }
              }
            }
          ]
        }
      }
    }
  
    // Make this search request
    const [os_search_response, success] = await os_client.search({index:"hive-accounts", body : search_query})
      .then(response => [response, response.statusCode === 200])
      .catch(err => {console.error("Error on Route/Search/Account: ", err); return [null, false]});
  
    if(!success)
      return res.send({status : "failed", err : "Search request failed enexpected", code : 0}).end();
  
    // Remap Results and send response
    let results = os_search_response.body.hits.hits;
    results = results.map(item => {return {name : item._source.name, score : item._score, json_metadata : {profile : item._source.profile}}});
    
    const total_matched_accounts = os_search_response.body.hits.total.value;
    const elapsedSeconds = (Date.now() - start_time) / 1000;
    res.send({status : "ok", accounts : results, time : elapsedSeconds, total_matched_accounts : total_matched_accounts}).end();
  })
  
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
    const by_author_mode = req.query.by_author_mode || false;
    const amount = Math.min(parseInt(req.body.amount || 7), 50);
    const tags = req.body.tags || [];
    const parent_permlinks = req.body.parent_permlinks || [];
    const wanted_langs = req.body.langs || [];
    const minus_days = req.body.minus_days || 0;
  
    //  * Validate form
    if(!Array.isArray(tags)){
      res.send({status : "failed", err : "tags is not an array", code : 1}).end()
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
    if(!Number.isInteger(minus_days)){
      res.send({status : "failed", err : "minus_days is not a number", code : 1}).end()
      return;
    }
  
    // Get source post from OpenSearch
    const source_post = await new Promise(async (resolve, reject) => { 
      try {
          const q = {
              "query": {
                "bool" : {
                  "must" : [
                    {"term" : {"author" : author}},
                    {"term" : {"permlink" : permlink}}
                  ]
                }
              },
              "_source" : {
                "includes" : ["doc_vector"]
              }
          }
          const response = await os_client.search({index : "hive-posts", body : q});
          if(!response.body.hits?.hits?.length) 
            throw Error("No post found");
          
          // We got the Post
          resolve(response.body.hits.hits[0]._source);
        } catch(err) {
          reject(err);
        }
      
    }).catch(err => {
      // Send error response
      if(err === "No post found" || (err.meta && err.meta.statusCode === 404))
        res.send({status : "failed", err : "Post not found", code : 2}).end();
      else {
        res.send({status : "failed", err : "Unknown unexpected error", code : 0}).end();
        console.error("Error in search.js/similar-post: " + err);
      }
  
      return null;
    });
  
    if(!source_post)
      return;
  
    // Build matching-query
    let similar_posts = await new Promise(async (resolve, reject) => {
      const get_query = (lang) => { 
        let query = {
          "size": amount,
          "query": {
              "script_score": {
                  "query": {
                    "bool" : {
                      "must" : [
                        { "exists" : { "field" : "doc_vector." + lang }}
                      ]
                    }
                  },
                  "script": {
                      "source": "knn_score",
                      "lang": "knn",
                      "params": {
                          "field": "doc_vector." + lang,
                          "query_value": source_post.doc_vector[lang],
                          "space_type": "cosinesimil"
                      }
                  }
              }
          },
          "_source": {
            "includes": [
              "author", "permlink"
            ]
          }
        }
  
        if(parent_permlinks.length > 0)
          query.query.script_score.query.bool.must.push({"terms" : {"parent_permlink" : parent_permlinks}})
        if(by_author_mode)
          query.query.script_score.query.bool.must.push({"term" : {"author" : author}})
        if(tags.length > 0)
          query.query.script_score.query.bool.must.push({"terms" : {"tags" : tags}})
        if(minus_days > 0)
          query.query.script_score.query.bool.must.push({"range" : {"timestamp" : {"gte" : "now-" + minus_days + "d"}}});
  
        return query;
      }
  
      // Start to get similar posts in each lang
      const similar_tasks = [];
      for(const lang of Object.keys(source_post?.doc_vector || {})){
        if(!source_post.doc_vector[lang] || source_post.doc_vector[lang].length !== 300)
          continue; // Mal formed doc-vector ==> this lang is not available
        if(wanted_langs.length > 0 && !wanted_langs.includes(lang))
          continue; // This lang is not wished
  
        similar_tasks.push(new Promise(async (resolve) => {
          const query = get_query(lang);
          const response = await os_client.search({index : "hive-posts", body : query, timeout : "30000ms"});
  
          if (response.statusCode === 200)
            resolve(response.body.hits.hits);
          else
            reject(response);
        }));
      }
  
      // Wait for them to finish and retrieve results
      let similar_posts = await Promise.all(similar_tasks);
      similar_posts = similar_posts.flat();
      similar_posts = similar_posts.map(document => { return {score : document._score, author : document._source.author, permlink : document._source.permlink, _id : document._id};});
      similar_posts = similar_posts.filter(post => post.permlink !== permlink);
      resolve(similar_posts);
    }).catch(err => {
      res.send({status : "failed", err : "Unknown unexpected error", code : 0}).end();
      console.error("Error in search.js/similar-post: " + err);
      return null;
    })  
  
    if(!similar_posts)
      return;
    
    // Sort by score
    similar_posts.sort((a, b) => (a.score > b.score) ? -1 : 1);
  
    // Filter duplicates out and maybe slice it
    similar_post_ids = similar_posts.map(o => o._id)
    similar_posts = similar_posts.filter(({_id}, index) => !similar_post_ids.includes(_id, index + 1))
    
    if(similar_posts.length > amount)
      similar_posts = similar_posts.slice(0, amount);
  
    // Send response
    const elapsedSeconds = (Date.now() - start_time) / 1000;
    res.send({status : "ok", posts : similar_posts, time : elapsedSeconds}).end();
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
    const by_author_mode = req.query.by_author_mode || false;
    const amount = Math.min(parseInt(req.body.amount || 7), 50);
    const tags = req.body.tags || [];
    const parent_permlinks = req.body.parent_permlinks || [];
    const wanted_langs = req.body.langs || [];
    const minus_days = req.body.minus_days || 0;
  
    //  * Validate form
    if(!Array.isArray(tags)){
      res.send({status : "failed", err : "tags is not an array", code : 1}).end()
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
    if(!Number.isInteger(minus_days)){
      res.send({status : "failed", err : "minus_days is not a number", code : 1}).end()
      return;
    }
  
    // Get source post from OpenSearch
    const source_post = await new Promise(async (resolve, reject) => { 
      try {
          const q = {
              "query": {
                "bool" : {
                  "must" : [
                    {"term" : {"author" : author}},
                    {"term" : {"permlink" : permlink}}
                  ]
                }
              },
              "_source" : {
                "includes" : ["doc_vector"]
              }
          }
          const response = await os_client.search({index : "hive-posts", body : q});
          if(!response.body.hits?.hits?.length) 
            throw Error("No post found");
          
          // We got the Post
          resolve(response.body.hits.hits[0]._source);
        } catch(err) {
          reject(err);
        }
      
    }).catch(err => {
      // Send error response
      if(err === "No post found" || (err.meta && err.meta.statusCode === 404))
        res.send({status : "failed", err : "Post not found", code : 2}).end();
      else {
        res.send({status : "failed", err : "Unknown unexpected error", code : 0}).end();
        console.error("Error in search.js/similar-by-author: " + err);
      }
  
      return null;
    });
  
    if(!source_post)
      return;
  
    // Build matching-query
    let similar_posts = await new Promise(async (resolve, reject) => {
      const get_query = (lang) => { 
        let query = {
          "size": amount,
          "query": {
              "script_score": {
                  "query": {
                    "bool" : {
                      "must" : [
                        { "exists" : { "field" : "doc_vector." + lang }},
                        { "term" : {"author" : author}},
                      ]
                    }
                  },
                  "script": {
                      "source": "knn_score",
                      "lang": "knn",
                      "params": {
                          "field": "doc_vector." + lang,
                          "query_value": source_post.doc_vector[lang],
                          "space_type": "cosinesimil"
                      }
                  }
              }
          },
          "_source": {
            "includes": [
              "author", "permlink"
            ]
          }
        }
  
        if(parent_permlinks.length > 0)
          query.query.script_score.query.bool.must.push({"terms" : {"parent_permlink" : parent_permlinks}})
        if(by_author_mode)
          query.query.script_score.query.bool.must.push({"term" : {"author" : author}})
        if(tags.length > 0)
          query.query.script_score.query.bool.must.push({"terms" : {"tags" : tags}})
        if(minus_days > 0)
          query.query.script_score.query.bool.must.push({"range" : {"timestamp" : {"gte" : "now-" + minus_days + "d"}}});
  
        return query;
      }
  
      // Start to get similar posts in each lang
      const similar_tasks = [];
      for(const lang of Object.keys(source_post?.doc_vector || {})){
        if(!source_post.doc_vector[lang] || source_post.doc_vector[lang].length !== 300)
          continue; // Mal formed doc-vector ==> this lang is not available
        if(wanted_langs.length > 0 && !wanted_langs.includes(lang))
          continue; // This lang is not wished
  
        similar_tasks.push(new Promise(async (resolve) => {
          const query = get_query(lang);
          const response = await os_client.search({index : "hive-posts", body : query, timeout : "30000ms"});
  
          if (response.statusCode === 200)
            resolve(response.body.hits.hits);
          else
            reject(response);
        }));
      }
  
      // Wait for them to finish and retrieve results
      let similar_posts = await Promise.all(similar_tasks);
      similar_posts = similar_posts.flat();
      similar_posts = similar_posts.map(document => { return {score : document._score, author : document._source.author, permlink : document._source.permlink, _id : document._id};});
      similar_posts = similar_posts.filter(post => post.permlink !== permlink);
      resolve(similar_posts);
    }).catch(err => {
      res.send({status : "failed", err : "Unknown unexpected error", code : 0}).end();
      console.error("Error in search.js/similar-by-author: " + err);
      return null;
    })  
  
    if(!similar_posts)
      return;
    
    // Sort by score
    similar_posts.sort((a, b) => (a.score > b.score) ? -1 : 1);
  
    // Filter duplicates out and maybe slice it
    similar_post_ids = similar_posts.map(o => o._id)
    similar_posts = similar_posts.filter(({_id}, index) => !similar_post_ids.includes(_id, index + 1))
    
    if(similar_posts.length > amount)
      similar_posts = similar_posts.slice(0, amount);
  
    // Send response
    const elapsedSeconds = (Date.now() - start_time) / 1000;
    res.send({status : "ok", posts : similar_posts, time : elapsedSeconds}).end();
  })

  return router;
}