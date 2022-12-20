const queryParser = require('express-query-int');
const bodyParser = require('body-parser')
const express = require('express')
const hivecrypt = require('hivecrypt');
const userAuth = require("../user_auth.js")
const crypto = require('crypto');

const { body, query, validationResult } = require('express-validator');

const config = require('../config');

const AVAILABLE_ACTIVITIES = {
    "post_opened" : {
        "user_collection" : "user_has_opened",
        "post_collection" : "post_is_opened",
        "required_metadata" : ["author", "permlink"],
        "delay" : 43200000 // half day in ms
    },
    "post_recommended" : {
        "user_collection" : "user_got_recommended",
        "post_collection" : "post_is_recommended",
        "required_metadata" : ["author", "permlink"],
        "delay" : 43200000 // half day in ms (but is unqiue per user)
    },
    "post_full_read" : {
        "user_collection" : "user_has_full_read",
        "post_collection" : "post_is_full_read",
        "required_metadata" : ["author", "permlink"],
        "delay" : 43200000 // half day in ms
    },
    "post_scrolled" : {
        "user_collection" : "user_has_scrolled",
        "post_collection" : "post_is_scrolled",
        "required_metadata" : ["author", "permlink"],
        "delay" : 3000 // 3s in ms
    },
    "post_clickthrough" : {
        "user_collection" : "user_has_clicked_through",
        "post_collection" : "post_is_clicked_through",
        "required_metadata" : ["origin_type", "origin_author", "origin_permlink", "target_author", "target_permlink"],
        "delay" : 43200000 // half day in ms
    },
    "post_survey" : {
        "user_collection" : "user_has_survey_answered",
        "post_collection" : "post_got_survey_answered",
        "required_metadata" : ["author", "permlink", "survey_answer"],
        "delay" : 43200000 // half day in ms
    },
}

module.exports = (os_client, mongo_client) => {
    const router = express.Router();
    router.use(bodyParser.json())
    router.use(bodyParser.urlencoded({ extended: true }));
    router.use(queryParser())

    const getUser = require("../users")(os_client, mongo_client);
    const documents = require('../documents')(os_client, mongo_client);

    router.post('/add', 
        // Authorize user
        body("username").isString().isLength({ min: 1 }).withMessage("Username is required"),
        body("user_id").isString().isLength({ min: 1 }).withMessage("user_id is required"),
        body('msg_encoded').isString().withMessage('msg_encoded is required'),
        // Request Params
        body("activity_type").isString().isIn(Object.keys(AVAILABLE_ACTIVITIES)).withMessage("Activity type not found"),
        // TODO: body("metadata").isObject().contains(AVAILABLE_ACTIVITIES[body("activity_type").escape().value].required_metadata).withMessage(`metadata is missing required fields: '${AVAILABLE_ACTIVITIES[body("activity_type").escape().value].required_metadata.join(", ")}'`),
        async (req, res) => {
            // Handle Errors
            const errors = validationResult(req);
            if (!errors.isEmpty()) {
                return res.status(422).json({ status : "failed", errors: errors.array() });
            }

            const {username, user_id, msg_encoded, activity_type, metadata} = req.body;

            // Check if user is authorized (also check pubMemoKey)
            const {publicMemoKey, deviceName, error : authError} = await userAuth.isAuthorized(username, msg_encoded, mongo_client);
            if(authError) {
                res.status(401).send({
                    status : "failed",
                    info : "User is not authorized",
                    error : authError
                }).end();
                return;
            }

            // Check that the user has activity infos
            const activity_infos = await userAuth.getActivityInfos(username, publicMemoKey, mongo_client, false);
            const hashedUserID = crypto.createHash('sha256').update(user_id).digest('hex');
            if(!activity_infos || activity_infos.userID !== hashedUserID) {
                res.status(401).send({
                    status : "failed",
                    info : "User has no activity infos or UserID mismatch"
                }).end();
                return;
            }

            // Parse the publicKey of the activity_infos (RSA)
            activity_infos.publicRSAKey = crypto.createPublicKey({
                key: activity_infos.publicRSAKey,
                format: 'pem',
                type: 'pkcs1'
            });
            
            // Remove metadata that is not required
            const metadataFiltered = Object.keys(metadata).reduce((acc, key) => {
                if(AVAILABLE_ACTIVITIES[activity_type].required_metadata.includes(key)) 
                    acc[key] = metadata[key]; // Field is required
                
                return acc;
            }, {});

            // Create unique anonymous user identifier (equal for all activities of the same user as long as the user_id / publicMemoKey is the same)
            // ==> sha256(username + publicMemoKey + userID)
            const anonymousUserID = crypto.createHash('sha256').update(username + publicMemoKey + user_id).digest('hex');

            // Firstly try to enter the activity in the post-activity collection because there are unencrypted timestamps,
            // when last activity is older than delay by this anonymousUserID
            const current_date = new Date();
            const last_date = new Date(current_date.getTime() - AVAILABLE_ACTIVITIES[activity_type].delay)
            const postActivityCol = mongo_client.db("activities").collection(AVAILABLE_ACTIVITIES[activity_type].post_collection);
            const postInsertResult = await postActivityCol.updateOne(
                // Query Part
                {
                    "userID" : anonymousUserID,
                    "metadata" : metadataFiltered,
                    "created" : {$gt : last_date}
                },
                // Update Part
                {
                    $setOnInsert : {
                        "created" : current_date
                    }
                },
                // Options Part
                {
                    upsert : true
                }
            );

            // Enter also into the user-activity collection if the post-activity collection was updated
            if(postInsertResult.upsertedCount === 1){
                // Encode data: Output should be same as the input keeps the same
                const metadata_encoded = crypto.publicEncrypt(activity_infos.publicRSAKey, Buffer.from(JSON.stringify(metadataFiltered))).toString('base64');
                const created_encoded = crypto.publicEncrypt(activity_infos.publicRSAKey, Buffer.from(current_date.toISOString())).toString('base64');

                // Create metadata identifier to have the same identifier for the same metadata for the same user
                const metadata_id = crypto.createHash('sha256').update(JSON.stringify(metadataFiltered) + user_id + publicMemoKey + username).digest('hex');

                const userActivityCol = mongo_client.db("activities").collection(AVAILABLE_ACTIVITIES[activity_type].user_collection);
                const userInsertResult = await userActivityCol.insertOne(
                    {
                        "username" : username,
                        "metadata_id" : metadata_id,
                        "metadata" : metadata_encoded,
                        "created" : created_encoded,
                        "index" : 0
                    }
                );

                // Update all activities by him by incrementing the counter
                const userActivityUpdateResult = await userActivityCol.updateMany(
                    {
                        "username" : username,
                    },
                    {
                        $inc : { "index" : 1 }
                    }
                );
            }   

            res.send({status : "ok"}).end();
        }
    );

    router.post('/view', 
        query("username").isString().isLength({ min: 1 }).withMessage("Username is required"),
        query('msg_encoded').isString().withMessage('msg_encoded is required'),
        body('private_activity_key').isString().withMessage('private_activity_key is required'),
        query("amount").optional().isInt({min : 1, max : 100}).default(25).withMessage("Amount must be between 1 and 100"),
        async (req, res) => {
        // Handle Errors
        const errors = validationResult(req);
        if (!errors.isEmpty()) {
            return res.status(422).json({ status : "failed", errors: errors.array() });
        }

        const {username, msg_encoded, amount} = req.query;
        const {private_activity_key} = req.body;

        // Check if user is authorized (also check pubMemoKey)
        const {publicMemoKey, deviceName, error : authError} = await userAuth.isAuthorized(username, msg_encoded, mongo_client);
        if(authError) {
            res.status(401).send({
                status : "failed",
                info : "User is not authorized",
                error : authError
            }).end();
            return;
        }

        // Check that the user has activity_infos
        const activity_infos = await userAuth.getActivityInfos(username, null, mongo_client, false);
        if(!activity_infos){
            res.status(401).send({
                status : "failed",
                info : "User has no activity_infos",
                error : "User has no activity_infos"
            }).end();
            return;
        }    
        

        // Check that the privateActivityKey is correct 
        if(!await userAuth.checkAcitivityKey(private_activity_key, activity_infos)){
            res.status(401).send({
                status : "failed",
                info : "wrong private_activity_key",
                error : "wrong private_activity_key"
            }).end();
            return;
        }
        
        const user = getUser(username, private_activity_key);

        const user_avg_task = user.getUserAverageReadingTime(username);

        // Get activities and author&permlink
        const scored_activities = await user.getScoredAccountActivities(250, amount, false, false);
        const author_permlinks = await mongo_client.db("hive").collection("comments")
            .find({_id : {$in : Object.keys(scored_activities).map((id) => parseInt(id))}})
            .project({author : 1, permlink : 1, _id : 1})
            .toArray();

        // Combine scores with author_permlinks
        const post_scores = author_permlinks.map(({author, permlink, _id}) => {
            return {
                author, permlink,
                score : scored_activities[_id] / 4 // 4 is the max score ==> percentage
            }
        }).sort((a, b) => b.score - a.score).slice(0, amount);

        res.send({status : "ok", result : {post_scores, user_avg : await user_avg_task}}).end()
    })

    return router;
}