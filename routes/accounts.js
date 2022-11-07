const queryParser = require('express-query-int');
const bodyParser = require('body-parser')
const express = require('express')
const hivejs = require("@hiveio/hive-js")
const hivecrypt = require('hivecrypt');
const crypto = require("crypto")
const { body, query, validationResult } = require('express-validator');
const userAuth = require("../user_auth.js")

// len(activity-timestamps) / word_tokens [< 1]
// - User likes posts, that have a high value for this equation OR does have a full_read activity for this post
// - User dislikes posts, that have a low value for this equation OR does not have a full_read activity for this post
// - What ist a low value for this equation? - maybe values that are 3/2 standard deviation under the average? This has to be tested with more Users

module.exports = (os_client, mongo_client) => {

  const router = express.Router();
  router.use(bodyParser.json())
  router.use(bodyParser.urlencoded({ extended: true }));
  router.use(queryParser())

  const {getFeed, getScoredAccountActivities} = require('../calc_feed')(os_client, mongo_client);


  const getAccountProfile = async (username) => {
      const search_query = {
          "size" : 1000,
          "query" : {
            "bool" : {
              "should": [
                {
                  "term" : {
                    "author" : username
                  }
                },
                {
                  "term" : {
                    "upvotes" : username
                  }
                }
              ]
            }
          },
          "_source" : {
              "includes" : [ "author", "categories", "language"]
          }
      }

      let categories = Array(46).fill(0);
      let languages = {};
      let cat_counter = 0;

      let batch = await os_client.search({index : "hive-posts", body : search_query, scroll : "10m"});
      while(true){
          // Get a batch
          if(!batch.body.hits.hits || batch.body.hits.hits.length === 0)
              break;

          // Add the batch and process it
          batch.body.hits.hits.forEach(post => {
              // Own Posts counts twice
              let multiplier = 1;
              if(post._source.author === username)
                  multiplier = 2;

              // Add the categories
              if(post["_source"]["category"] && Array.isArray(post["_source"]["category"]) && post["_source"]["category"].length === 46){            
                  categories += post["_source"]["category"] * multiplier;
                  cat_counter += 1 * multiplier;
              }

              // Add the languages
              if(post["_source"]["language"]){
                  post["_source"]["language"].forEach(({x, lang}) => {
                      if(!languages[lang])
                          languages[lang] = 0;

                      languages[lang] += x * multiplier;
                  })
              }
          });

          // Get the next batch
          const scroll_id = batch.body['_scroll_id']
          batch = await os_client.scroll({scroll_id : scroll_id, scroll : "10m"})
      }

      // Calculate the average for the categories
      if(cat_counter > 0)
          categories = categories.map(x => x / cat_counter);

      // Calculate the percentage for the languages and filter out langs with below 25%
      const total_score = Object.values(languages).reduce((a, b) => a + b, 0);
      if(total_score > 0) {
          Object.keys(languages).forEach(lang => {
              languages[lang] = languages[lang] / total_score;
          });
      }
      const filtered_langs = Object.keys(languages).filter(lang => languages[lang] > 0.25);

      return {categories : categories, langs : filtered_langs};
  }

  router.get('/', async (req, res) => {
      // Required
      const username = req.query.username;
      if(!username || username.length < 2){
          res.send({status : "failed", err : {"msg" : "Please give valid username!"}, code : 1}).end()
          return;
      }

      // We do not need to check if the user exists, because we will just get an empty profile if he does not exist

      // Account does exist
      res.send({status : "ok", msg : "Account is available", profile : (await getAccountProfile(username))}).end()
  })
    
  router.get('/session-id', async (req, res) => {
      // Required
      const username = req.query.username;
      if(!username || username.length < 2){
          res.send({status : "failed", err : {"msg" : "Please give valid username!"}, code : 1}).end()
          return;
      }

      // Create ID containing the username and some random bytes
      const data = Buffer.from(username + crypto.randomBytes(64).toString('hex'), 'utf8');
      const id = crypto.createHash('sha256').update(data).digest('hex');

      res.send({status : "ok", session_id : id}).end()
  });

  router.get('/register-device', 
    query('username').isLength({ min: 3, max : 25 }).withMessage('Username is required'),
    async (req, res) => {      
      // Check Errors
      const errors = validationResult(req);
      if (!errors.isEmpty()) {
        return res.status(422).json({ errors: errors.array() });
      }

      // Parse the query string
      const {username, deviceName } = req.query;
      const currentDateTime = new Date();

      // Check if the user exists
      const hiveUser = await hivejs.api.callAsync("condenser_api.get_accounts", [[username]]);
      if(!hiveUser || hiveUser.length === 0){
          res.send({status : "failed", err : {"msg" : "User does not exist!"}, code : 1}).end()
          return;
      }

      const deviceCollection = mongo_client.db("accounts").collection("devices");
      const publicMemoKey = hiveUser[0].memo_key;

      // Check that...
      //  - the last device-register by this user is older than 10 seconds ago 
      //  - the user has fewer than 25000 devices registered
      const check_results = await Promise.all([
        deviceCollection.findOne({username : username, createdAt : {$gt : new Date(currentDateTime - 10000)}}),
        deviceCollection.countDocuments({username : username}).then(count => count >= 25000)
      ]);
      if(check_results[0]){
          res.send({status : "failed", err : {"msg" : "You can only register one device every 10 seconds!"}, code : 2}).end()
          return;
      }
      if(check_results[1]){
          res.send({status : "failed", err : {"msg" : "You can only register a specific amount of devices per week!"}, code : 4}).end()
          return;
      }

      // Generate unique device Key
      const deviceKey = crypto.randomBytes(32).toString('hex'); // 32 bytes == 64 hex chars
      const hashedDeviceKey = crypto.createHash('sha256').update(deviceKey).digest('hex');

      // Encode Message for this Username and Public Memo Key
      const message = "#" + JSON.stringify({deviceKey : deviceKey, createdAt : currentDateTime});
      const msg_encoded = hivecrypt.encode(process.env.ActionChain_PRV_MEMO, publicMemoKey, message);

      // Save the hashedDeviceKey in the database with the username, publicMemoKey and timestamp
      const document = {
        username : username,
        deviceName : deviceName,
        deviceKey : hashedDeviceKey,
        publicMemoKey : publicMemoKey,
        createdAt : currentDateTime
      }
      const result = await deviceCollection.insertOne(document);
      if(!result.insertedCount){
          res.send({status : "failed", err : {"msg" : "Could not save the deviceKey!"}, code : 1}).end()
          return;
      }

      // Get Activity Infos
      const activity_info = await userAuth.getActivityInfos(username, publicMemoKey, mongo_client);

      res.send({
        status : "ok",
        info : "Message encoded",
        msg_encoded : msg_encoded,
        activity_info : activity_info
      })
  });

  router.get('/verify-device',
    query('username').isLength({ min: 2, max : 25 }).withMessage('Username is required'),
    query('msg_encoded').isLength({ min: 100, max : 500 }).withMessage('msg_encoded is required (min: 100; max : 500)'),
    query('user_id').isLength({ min: 2 }).withMessage('user_id is required'),
    async (req, res) => {
      // Check Errors
      const errors = validationResult(req);
      if (!errors.isEmpty()) {
        return res.status(422).json({ errors: errors.array() });
      }

      // Parse the query string and decode the message
      const {username, msg_encoded, user_id } = req.query;
      const msg_decoded = hivecrypt.decode(process.env.ActionChain_PRV_MEMO, (msg_encoded[0] !== "#" ? "#" + msg_encoded : msg_encoded));

      // Parse the msg and hash the deviceKey
      const {deviceKey, createdAt} = JSON.parse(msg_decoded.slice(1));
      const hashedDeviceKey = crypto.createHash('sha256').update(deviceKey).digest('hex');

      // Check if createdAt is not older/newer than 1 minute
      if(new Date(createdAt) < new Date(Date.now() - 60 * 1000) || new Date(createdAt) > new Date(Date.now() + 60 * 1000)){
          res.send({status : "failed", err : {"msg" : "Message is too old!"}, code : 1}).end()
          return;
      }

      // Check if the deviceKey is registered for this user
      const deviceCollection = mongo_client.db("accounts").collection("devices");
      const device = await deviceCollection.findOne({username : username, deviceKey : hashedDeviceKey}, {projection : {deviceName : 1, publicMemoKey : 1}});
      if(!device){
          res.send({status : "failed", err : {"msg" : "DeviceKey is not registered!"}, code : 2}).end()
          return;
      }

      // Check if activity_info is valid: user_id, username, publicMemoKey matches?
      const hashedUserID = crypto.createHash('sha256').update(user_id).digest('hex');
      const activity_info = await userAuth.getActivityInfos(username, device.publicMemoKey, mongo_client, false);
      if(!activity_info || activity_info.userID !== hashedUserID || activity_info.publicMemoKey != device.publicMemoKey){
          res.send({status : "failed", err : {"msg" : "Activity info is not valid!"}, code : 3}).end()
          return;
      }

      // Success, the deviceKey is registered for this user
      res.send({status : "ok", deviceName : device.deviceName, username : username}).end()
    }
  );


  return router;
}