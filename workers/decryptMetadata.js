const hivecrypt = require('hivecrypt');
const workerpool = require('workerpool');
const crypto = require('crypto');

function decryptMetadata(prvActivityKey, doc){
    try
    {
        const metadata_enc = crypto.privateDecrypt(prvActivityKey, Buffer.from(doc.metadata, "base64")).toString();
        const metadata = JSON.parse(metadata_enc);

        const created_enc = crypto.privateDecrypt(prvActivityKey, Buffer.from(doc.created, "base64")).toString();
        const created = new Date(created_enc);

        return {
          _id: metadata,
          timestamp_count: doc.timestamp_count,
          index: doc.index_min,
          created: created,
        };
      } 
      catch 
      {
        return null;
      }
  } 

// create a worker and register public functions
workerpool.worker({
    decryptMetadata: decryptMetadata,
});