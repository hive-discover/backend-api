const makeApp = require('./app.js');

module.exports = () => {
    const os_client = {
        search: jest.fn()
    };
    
    // mongo_client.db("").collection("");
    const mongo_client = {
        db: jest.fn(() => ({
            collection: jest.fn(() => ({
                find: jest.fn(() => ({
                    toArray: jest.fn(() => [
                        {
                            _id: "5e8f8f8f8f8f8f8f8f8f8f8f",
                            name: "test",
                            description: "test",
                            url: "test",
                            image: "test",
                            tags: ["test"],
                            created_at: "test",
                            updated_at: "test"
                        }
                    ])
                }))
            }))
        }))
    }

    return [
        makeApp(os_client, mongo_client),
        os_client,
        mongo_client
    ];
}