const crypto = require('crypto');
const hivecrypt = require('hivecrypt');
const hiveSigner = require('hivesigner')


module.exports.authorizeUser = async ({username, access_token, keychain_signed_msg}) => {
    // Check for KeyChain Auth
    if(keychain_signed_msg){
        try{
            const plain_msg = hivecrypt.decode(process.env.ActionChain_PRV_POSTING, keychain_signed_msg);
            if(plain_msg === "#" + username) // correct
                return [true, "keychain", ""];
            
            return [false, "keychain", "Signed message is not valid for this username"];
        } catch(excetion){
            return [false, "keychain", "Error while decoding keychain signed message"];
        }       
    }

    // Check HiveSigner Auth
    if(access_token){      
        const hs_client = new hiveSigner.Client({ app: 'action-chain', scope: ['login'], accessToken : access_token});
        return await new Promise(resolve => {
            hs_client.me((err, result) => {
                if(err || !result)
                    return resolve([false, "hivesigner", err?.error || "Error while getting user info"]);
                    
                if(result.user === username) // Correct
                    return resolve([true, "hivesigner", ""]);
                else
                    return resolve([false, "hivesigner", "User profile is not valid for this acess token"]);                
            });
        });
    }

    return [false, "", "No auth method provided"];
}

function tryDecodeAndParseMessage(msg_encoded){
    try {
        // Add hashtag to the message (optional) and decode it
        msg_encoded = msg_encoded[0] !== "#" ? "#" + msg_encoded : msg_encoded;
        const msg_decoded = hivecrypt.decode(process.env.ActionChain_PRV_MEMO, msg_encoded);

        // Parse the message
        const {deviceKey, createdAt} = JSON.parse(msg_decoded.slice(1));
        return {deviceKey, createdAt};
    } catch(excetion){
        // Message is not valid
        return {error : "Message not valid / parseable"};
    }

    return {error : "Unknown Exception raised"};
}

module.exports.isAuthorized = async (username, msg_encoded, mongo_client, pubMemoKey = null) => {
    // Decode and Parse encoded Message
    const {deviceKey, createdAt, error} = tryDecodeAndParseMessage(msg_encoded);
    if(error)
        return {error : error};
    
    // TODO: Check if createdAt is not older/newer than 5 minute
    // if(new Date(createdAt) < new Date(Date.now() - 60 * 1000 * 5) || new Date(createdAt) > new Date(Date.now() + 60 * 1000 * 5))
    //    return {error : "Timestamp not valid"}; // Message to old or to new
    

    // Hash Device Key and prepare Query
    const hashedDeviceKey = crypto.createHash('sha256').update(deviceKey).digest('hex');
    const deviceCollection = mongo_client.db("accounts").collection("devices");
    const deviceQuery = {
        username : username,
        deviceKey : hashedDeviceKey,
        ...(pubMemoKey ? {pubMemoKey} : {}) // If pubMemoKey is provided, add it to the query
    }

    // Check if the deviceKey is registered for this user
    const device = await deviceCollection.findOne(deviceQuery, {projection : {deviceName : 1, publicMemoKey : 1}});
    if(!device)
        return {error : "Device not registered"}; // Device not registered for this user (or pubMemoKey is not valid)

    // Success, the deviceKey is registered for this user (and pubMemoKey is valid)
    return device;
}

module.exports.getActivityInfos = async (username, pubMemoKey, mongo_client, generate_if_needed = true) => {
    const hashedUsername = crypto.createHash('sha256').update(username).digest('hex');
    const activityCollection = mongo_client.db("accounts").collection("activity_info");
    
    const activity = await activityCollection.findOne({username : hashedUsername});
    if(activity)
        return activity; // Activity infos already set
    if(!generate_if_needed)
        return null; // Activity infos not set and generate_if_needed is false

    // Generate Activity Infos
    const { publicKey, privateKey } = crypto.generateKeyPairSync("rsa", {
        modulusLength: 2048,
    });
    const exportedPublicKey = publicKey.export({type: 'pkcs1', format: 'pem'});
    const exportedPrivateKey = privateKey.export({type: 'pkcs1', format: 'pem'});
    const userID = crypto.randomBytes(32).toString('hex'); // 32 bytes == 64 hex chars
    const hashedUserID = crypto.createHash('sha256').update(userID).digest('hex');
    const infoMessage = hivecrypt.encode(process.env.ActionChain_PRV_MEMO, pubMemoKey, "#" + JSON.stringify({userID, publicKey : exportedPublicKey, privateKey : exportedPrivateKey}));

    // Save Activity Infos and return document
    const doc = {
        username : hashedUsername,
        userID : hashedUserID,
        publicRSAKey : exportedPublicKey, 
        infoMessage : infoMessage,
        publicMemoKey : pubMemoKey
    };

    await activityCollection.insertOne(doc);
    return doc;
}

module.exports.checkAcitivityKey = async (privateActivityKey, activity_info) => {
    if(!activity_info)
        return false; // Activity infos not set

    // Encrypt and decrypt a random string to check if the privateActivityKey is valid
    try{
        const randomString = crypto.randomBytes(32).toString('hex'); // 32 bytes == 64 hex chars
        const encryptedString = crypto.publicEncrypt(activity_info.publicRSAKey, Buffer.from(randomString));
        const decryptedString = crypto.privateDecrypt(privateActivityKey, encryptedString).toString();
        return decryptedString === randomString;
    }catch{
        return false;
    }
}