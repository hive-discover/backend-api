const request = require('request');


const run_request = (options) => {
    return new Promise((resolve) => {
        request(options, (error, response, body) => {
            resolve({error, response, body});
        });
    });
}

module.exports = async (options, ok_status_codes=['*'], max_retries=7, delay_ms=25) => {
    let first_error = null, last_error = null;

    // Retry loop until we get a response with an acceptable status code or we run out of retries
    for (let i = 0; i < max_retries; i++) {
        // Try request
        const {error, response, body} = await run_request(options);

        // Check if request was successful (no error, response exists and status code is ok)
        if(!error && response && (ok_status_codes.includes(response.statusCode) || ok_status_codes.includes('*'))) {
            return {response, body};
        }

        // Log error
        if(error){
            if(!first_error)
                first_error = error;
            last_error = error;
        }

        // Request failed, run again after delay       
        await new Promise(resolve => setTimeout(resolve, delay_ms));
    }

    // Return first and last error
    return {error : [first_error, last_error]};
}

