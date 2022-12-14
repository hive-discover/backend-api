const queryParser = require('express-query-int');
const bodyParser = require('body-parser')
const express = require('express')
const { body, query, validationResult } = require('express-validator');
const request = require('request');
const Base58 = require('bs58');

const config = require('../config');

module.exports = (os_client, mongo_client) => {

  const router = express.Router();
  router.use(bodyParser.json())
  router.use(bodyParser.urlencoded({ extended: true }));
  router.use(queryParser())

  router.post('/text', async (req, res) => {
      const start_time = Date.now();
      
      // Get Search input
      const text_query = req.body.query, full_data = req.body.full_data;
      const amount = Math.min(Math.abs(parseInt(req.body.amount || 10)), 100);
          
      if(!text_query) {
          res.send({status : "failed", err : "Query is null", code : 1}).end()
          return;
      }

      const projection = full_data ? {author : 1, permlink : 1, created : 1, title : 1, "json_metadata.image" : 1} : {author : 1, permlink : 1};
      const gather_amount = Math.max(amount * 2, 100);

      const tag_search = async () => {
        const mongo_query = {
          stockimage : true,
          stockimage_muted : {$ne : true},
          stockimage_tags : {$in : text_query.split(" ").map(tag => tag.toLowerCase())}
        }

        return await mongo_client.db("hive").collection("comments")
                    .find(mongo_query, {projection : projection, sort : {created : -1}, limit : gather_amount})
                    .toArray();
      }

      const text_searching = async () => {
        // Create Search Query
        const mongo_query = {
          stockimage : true,
          stockimage_muted : {$ne : true},
          $text : {$search : text_query}
        }

        return await mongo_client.db("hive").collection("comments")
                    .find(mongo_query, { score: { $meta: "textScore" } })
                    .project({...projection, score : { $meta: "textScore" }})
                    .sort({ score: { $meta: "textScore" } })
                    .limit(gather_amount)
                    .toArray();
      }

      const [tag_results, text_results] = await Promise.all([tag_search(), text_searching()]);

      // Set all tag_results scores to the ceiled maximum value + 1 of text_results
      const max_text_score = text_results.length > 0 ? Math.ceil(text_results[0].score) : 0;
      tag_results.forEach(item => item.score = max_text_score + 1);

      // Combine results by adding the scores
      let search_result = [...tag_results, ...text_results];
      search_result = search_result.reduce((acc, item) => {
        const found = acc.find(x => x.author === item.author && x.permlink === item.permlink);
        if(found)
          found.score += item.score;
        else
          acc.push(item);
        
        return acc;
      }, []);

      // Get total amount of results
      let total_amount = search_result.length;
      if(total_amount === gather_amount * 2)
        total_amount = total_amount.toString() + "+";


      // Sort by score and take the top amount
      search_result = search_result.filter(item => item.json_metadata?.image?.length >= 1);
      search_result = search_result.sort((a, b) => b.score - a.score).slice(0, amount);

      // Rename created to timestamp and json_metadata.image to images
      search_result = search_result.map(item => {
        return {...item, timestamp : item.created, images : item.json_metadata.image, created : undefined, json_metadata : undefined};
      });

            
      // Send response
      const elapsedSeconds = (Date.now() - start_time) / 1000;
      res.send({status : "ok", posts : search_result, time : elapsedSeconds});
    });

  // router.post('/text-to-image', 
  //   body('text').isString().notEmpty().withMessage("text is null"),
  //   body('amount').optional({ checkFalsy: true }).isInt({min : 1, max : 100}).default(10).withMessage("Amount is not valid"),
  //   body('page_number').optional({ checkFalsy: true }).isInt({min : 0}).default(0).withMessage("Page number is not valid"),
  //   body('sorting').optional({ checkFalsy: true }).isString().default("smart").withMessage("Sorting is not valid"),

  //   async (req, res) => {
  //     // Validate Form
  //     let errors = validationResult(req);
  //     if (!errors.isEmpty()) {
  //       return res.status(422).json({ status : "failed", err : errors.array(), code : 0 });
  //     }

  //     const start_time = Date.now();
  //     const { text, amount, page_number, sorting } = req.body;

  //     const total_results = new Promise(async (resolve) => {
  //       // Count all documents in hive-imgs
  //       const resp = await os_client.count({index : "hive-imgs", body : {"query" : {"match_all" : {}}}});
  //       if(resp.statusCode !== 200)
  //         return resolve(null);

  //       resolve(resp.body.count);
  //     })


  //     // Get Text Embeddings from CLIP-API
  //     const getTextEmbedding = async(fail_counter = 0) => {
  //       const {response, body, error} = await new Promise((resolve) => {
  //         request({url : config.CLIP_API_ADDRESS + "/encode-text", qs : {text : text}, method : "GET"}, (error, response, body ) => resolve({response, body, error}));
  //       });

  //       if(response.statusCode !== 200 || error){
  //         if(fail_counter > 5) 
  //           return null;

  //         // Sleep 200ms and try again
  //         await new Promise(resolve => setTimeout(resolve, 200));
  //         return await getTextEmbedding(fail_counter + 1);
  //       }

  //       // Return the embedding as array
  //       return {embedding : JSON.parse(body), fail_counter};
  //     }

  //     const {embedding : embedded_text, fail_counter : emb_fail_counter} = await getTextEmbedding();
  //     if(!embedded_text)
  //       return res.send({status : "failed", err : "Embedding request failed", code : 1, fail_counter : emb_fail_counter}).end();

  //     // Do the search in opensearch
  //     let search_query = {
  //       "size" : amount,
  //       "from" : (page_number || 0) * amount,
  //       "query" : {
  //         "script_score": {
  //             "query": { "match_all": {} },
  //             "script": {
  //                 "source": "knn_score",
  //                 "lang": "knn",
  //                 "params": {
  //                     "field": "clip_vector",
  //                     "query_value": embedded_text,
  //                     "space_type": "cosinesimil"
  //                 }
  //             }
  //         }     
  //     },
  //     "_source" : {
  //       "includes" : ["image_hash"]
  //       }  
  //     }

  //     // Make this search request
  //     const os_img_resp = await os_client.search({index:"hive-imgs", body : search_query});
  //     if(os_img_resp.statusCode !== 200)
  //       return res.send({status : "failed", err : "Search request for images failed unexpected", code : 2}).end();

  //     // Get the results
  //     let search_result = os_img_resp.body.hits.hits.map(hit => {return {image_hash : hit._source.image_hash, score : hit._score}});
  //     search_result = search_result.map(item => { if (!Array.isArray(item.image_hash)) item.image_hash = [item.image_hash]; return item; });

  //     // Decode b58 image hashes to the original image urls
  //     const hashes_to_urls = Object.fromEntries(search_result.map(search_item => {
  //       if(!Array.isArray(search_item.image_hash))
  //         search_item.image_hash = [search_item.image_hash];

  //       return search_item.image_hash.map(encoded => {
  //         const decoded = Base58.decode(encoded);
  //         return [encoded, Buffer.from(decoded).toString('utf-8')];
  //       });
  //     }).flat(1));

  //     // Get one post (the newest post where the image is used)
  //     search_query = {
  //       "size" : Object.keys(hashes_to_urls).length,
  //       "query" : {
  //         "terms" : {
  //           "image.keyword" : Object.values(hashes_to_urls)
  //         }
  //       },
  //       "sort" : [
  //         {"timestamp" : "desc"}
  //       ],
  //       "_source" : {
  //         "includes" : ["author", "permlink", "image"]
  //       }
  //     }
  //     const os_posts_resp = await os_client.search({index : "hive-posts", body : search_query});
  //     if(os_posts_resp.statusCode !== 200)
  //       return res.send({status : "failed", err : "Search request for posts failed unexpected", code : 3}).end();

  //     // First Post is the newest post
  //     let posts = os_posts_resp.body.hits.hits.map(hit => {return {author : hit._source.author, permlink : hit._source.permlink, image : hit._source.image}});

  //     // Combine search_results with posts (image_hash to post)
  //     search_result = search_result.map(search_item => {
  //       // Find the post-index where the image is latest used (lowest index ==> newest post)
  //       const {img_url, post_idx : lowest_post_index} = search_item.image_hash
  //         .map(hash => {
  //           return {img_url : hashes_to_urls[hash], post_idx : posts.findIndex(post => post.image.includes(hashes_to_urls[hash]))}}
  //         )
  //         .filter(
  //           item => item.post_idx !== -1
  //         )
  //         .sort(
  //           (a, b) => a.post_idx - b.post_idx
  //       )[0] || {img_url : null, post_idx : -1};

  //       if(lowest_post_index === -1 || !img_url)
  //         return null;

  //       return {...posts[lowest_post_index], score : search_item.score, img_url};
  //     }).filter(item => item !== null);

  //     // Send response
  //     const elapsedSeconds = (Date.now() - start_time) / 1000;
  //     res.send({status : "ok", posts : search_result, total : await total_results, time : elapsedSeconds});
  // });

  // TODO: Replace it when the new search is ready
  
  const mongodb = require('../database.js')
  router.post('/similar', async (req, res) => {
      return res.send({status : "failed"});
      // Get Search input
      // const img_desc = req.body.img_desc, full_data = req.body.full_data;
      // const amount = Math.min(parseInt(req.body.amount || 100), 1000);

      // if(!img_desc) {
      //     res.send({status : "failed", err : "Query is null", code : 1}).end()
      //     return;
      // }
      // let countposts_text = mongodb.countDocumentsInCollection("post_info", {}, "images");

      // // Prepare Query and Request
      // const req_options = {
      //   'method': 'POST',
      //   'url': 'https://sim-image-api.hive-discover.tech/similar-searching',
      //   'headers': {
      //     'Content-Type': 'application/json'
      //   },
      //   body: JSON.stringify({query : img_desc, amount : amount})
      // };
      
      // try{
      //   new Promise(async (resolve, reject) => {

      //       // Send query to NswAPI, retrieve response and cache it
      //       let {error, response, body} = await managed_request(req_options, [200]);
      //       if (error) throw error;
            
      //       // Parse Response Body
      //       body = JSON.parse(body);
      //       if(body.status !== "ok" || !body.results) throw (body.error || "Something failed");    

      //       // Resolve body posts and cache them
      //       body.results = body.results.map(item => {return parseInt(item, 10)}) 
      //       if(body.results.length > amount) // Maybe it is to long
      //         body.results = body.results.slice(0, amount);
          
      //         resolve(body.results);
      //   }).then(async (posts) =>{
      //     // Check if full_data, else get only authorperm
      //     let cursor = null;
      //     if(full_data)
      //       cursor = await mongodb.findManyInCollection("post_info", {_id : {$in : posts}}, {}, "images")
      //     else
      //       cursor = await mongodb.findManyInCollection("post_info", {_id : {$in : posts}}, {projection : {author : 1, permlink : 1}}, "images")

      //     for await(const post of cursor) {
      //       // Set on correct index
      //       posts.forEach((elem, index) => {
      //         if(elem === post._id){
      //           posts[index] = post
      //         }
      //       });
      //     }   
          
      //     // Remove errors (when the elem is an _id (a number)) and return
      //     posts = posts.filter(elem => !Number.isInteger(elem));
      //     return posts; 
      //   }).then(async (posts) =>{
      //     // Send response
      //     res.send({status : "ok", posts : posts, total : await countposts_text, time : 0});
      //   });
      // }catch(err) {
      //   console.error("Error in images.js/similar: " + err);
      //   res.send({status : "failed", err : err, code : 0}).end()
      // }
  });

  router.get('/similar-url', async (req, res) => {
    res.send({status : "ok", sim_objs : [], time : 0});
  });

  router.get('/used', async (req, res) => { 
      return res.send({status : "failed"});
    // // Get Search input
    // const username = req.query.username;
    // const redis_key_name = "search-used-image-" + username;
    // if(!username) {
    //     res.send({status : "failed", err : "Query is null", code : 1}).end()
    //     return;
    // }

    // logging.writeData(logging.app_names.general_api, {"msg" : "Image - Usage Data", "info" : {
    //   "username" : username
    // }});

    // // Check if it is cached
    // let posts = await new Promise(resolve => {config.redisClient.get(redis_key_name, async (error, reply) => {
    //   if(reply) // We got a cached result
    //     resolve(JSON.parse(reply));
    //   else
    //     resolve([]);
    //   });
    // });// {_id : x, img : []}

    // if(posts.length === 0){
    //   // It is not cached, so we need to get it from the database
    //   // Find all images of this user
    //   let img_urls = new Set();
    //   let cursor = await mongodb.findManyInCollection("post_info", {author : username}, {}, "images");
    //   for await(const post of cursor)
    //     post.images.forEach(elem => img_urls.add(elem));

    //   // Remove failed images
    //   img_urls.delete("");
    //   img_urls.delete(null)
    //   img_urls.delete(undefined)
    //   img_urls.delete(" ");

    //   // Find all posts where at least one image from him is used
    //   cursor = await mongodb.findManyInCollection(
    //                         "post_raw", 
    //                         {"raw.json_metadata.image" : {"$in" : Array.from(img_urls)}, "raw.author" : {$ne : username}}, 
    //                         {projection : {"raw.author" : 1, "raw.permlink" : 1, "raw.title" : 1, images : "$raw.json_metadata.image", timestamp : 1}}, 
    //                         "hive-discover"
    //                       );
    //   for await(const post of cursor.sort({timestamp : -1})){
    //     let used_imgs = new Set(post.images.filter(elem => img_urls.has(elem)));
    //     posts.push({
    //       author : post.raw.author,
    //       permlink : post.raw.permlink,
    //       title : post.raw.title,
    //       images : Array.from(used_imgs)
    //     });
    //   }

    //   // Then cache for 10 min
    //   config.redisClient.set(redis_key_name, JSON.stringify(posts), (err, reply) => {if (err) console.error(err);});
    //   config.redisClient.expire(redis_key_name, 60*10);
    // }
    
      
    // // Send response
    // const elapsedSeconds = 0;
    // res.send({status : "ok", posts : posts, time : elapsedSeconds});
    
  });

  router.get('/mute-post', async (req, res) => { 
    // Get Search input
    const author = req.query.author.replace("@", "");
    const permlink = req.query.permlink.replace("/", "");
    const password = req.query.password;
    if(!author || !permlink || !password) {
        res.send({status : "failed", err : "Query is null", code : 1}).end()
        return;
    }

    // Check Password
    if(password !== process.env.IMAGE_API_MUTING_PASSWD){
      res.send({status : "failed", err : "Wrong Password", code : 2}).end()
      return;
    }

    // Check if it does exist
    const post_info = await mongodb.findOneInCollection("post_info", {author : author, permlink : permlink}, "images");
    if(!post_info) {
        res.send({status : "failed", err : "Post does not exist", code : 3}).end()
        return;
    }

    // Check if it is already muted (normally not possible)
    const muted_post = await mongodb.findOneInCollection("muted", {author : author, permlink : permlink}, "images");
    if(muted_post){
      res.send({status : "ok", msg : "Post is already muted"}).end()
      return;
    }

    // Mute and Delete the post
    await Promise.all([
      mongodb.insertOne("muted", {author : author, permlink : permlink, type : "post"}, "images"),
      mongodb.deleteMany("post_info", {_id : post_info._id}, "images"),
      mongodb.deleteMany("post_text", {_id : post_info._id}, "images"),
      mongodb.deleteMany("post_data", {_id : post_info._id}, "images")
    ]);

    // Remove dangling Images (imgs which target no post)
    const pipeline = [
      {
        '$lookup': {
          'from': 'post_info', 
          'localField': 'url', 
          'foreignField': 'images', 
          'as': 'info'
        }
      }, {
        '$match': {
          'info._id': {
            '$exists': false
          }
        }
      }, {
        '$project': {
          '_id': 1
        }
      }
    ]

    // Get Ids and Remove these dangling images
    const dangling_img_ids = await mongodb.aggregateInCollection("img_data", pipeline, "images") // Get img-ids
                                    .then(async (cursor) => {return await cursor.toArray();}) // Convert to Array
                                    .then(arr => arr.map(elem => elem._id)); // Get only the ids
    await mongodb.deleteMany("img_data", {_id : {$in : dangling_img_ids}}, "images"); 

    // Send response
    res.send({status : "ok"});
  });

  router.get('/mute-list', async (req, res) => { 
    const results = await mongodb.findManyInCollection("muted", {}, {}, "images")
      .then(async (cursor) => {return await cursor.toArray();});

    // Send response
    res.send({status : "ok", result : results});
  });

  router.get('/post-info', async (req, res) => {

    // Send response
    res.send({
      status : "failed", 
    });
  });

  return router;
}