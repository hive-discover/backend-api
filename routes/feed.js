const queryParser = require('express-query-int');
const bodyParser = require('body-parser')
const express = require('express')
const { body, validationResult } = require('express-validator');
const userAuth = require("../user_auth.js")
const crypto = require("crypto")

module.exports = (os_client, mongo_client) => {

    const router = express.Router();
    router.use(bodyParser.json())
    router.use(bodyParser.urlencoded({ extended: true }));
    router.use(queryParser())

    const recommender = require('../recommender')(os_client, mongo_client);

    router.post(
        '/', 
        body('username').isLength({ min: 3, max : 25 }).withMessage('Username is required'),
        body('private_activity_key').isString().withMessage('private_activity_key is required'),
        body('amount').optional().isInt({ min: 1, max: 100 }).default(7),
        // Filters and Options (optional)
        body('filter').optional().isObject().withMessage('Filter is not an object'),
        body('filter.parent_permlinks').optional().isArray().default([]).withMessage('Filter.parent_permlinks is not an array'),
        body('filter.tags').optional().isArray().default([]).withMessage('Filter.tags is not an array'),
        body('filter.langs').optional().isArray().default([]).withMessage('Filter.langs is not an array'),
        body('filter.distraction').optional().isInt({ min: 0, max: 100 }).default(0).withMessage('Filter.distraction is not an integer between 0 and 100'),
        async (req, res) => {
            // Handle Errors
            const errors = validationResult(req);
            if (!errors.isEmpty()) {
                return res.status(422).json({ status : "failed", errors: errors.array() });
            }

            const {username, private_activity_key, amount, filter} = req.body;

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
 
            const feed = await recommender.getFeed(username, private_activity_key, amount, (filter || {}));
            res.json({status : "ok", posts : feed});
        }
    );


    return router;
}
