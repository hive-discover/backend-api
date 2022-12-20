

module.exports = [
    // [path, createHandler(os_client, mongo_client)]
    ["/accounts", require("./accounts.js")],
    ["/search", require("./search.js")],
    // ["/smart-search", require("./smart_search.js")],
    ["/images", require("./images.js")],
    ["/activities", require("./activities.js")],
    ["/feed", require("./feed.js")],
]