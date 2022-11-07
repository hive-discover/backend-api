const request = require('supertest');
const getTestApp = require('../testApp.js'); 

describe("POST /search/posts", () => {

    const [app, os_client, mongo_client] = getTestApp();

     // Reset mock function for each test
     beforeEach(() => {
        os_client.search.mockClear();
    });

    it("Fail when no query is provided with code 1", async () => {
        const res = await request(app).post("/search/posts");
        expect(res.body.status).toBe("failed");
        expect(res.body.code).toBe(1);
    });

    const post_amount = 10
    const query = "bitcoins"
    os_client.search.mockReturnValue(Promise.resolve({
        statusCode : 200,
        body : {
            hits : {
                total : {value : post_amount},
                hits : [...Array(post_amount).fill({_source : {author : "foo", permlink : "bar"}, _score : 1, highlight : {}})]
            }
        }
    }));

    it("status ok", async () => {
        const res = await request(app).post("/search/posts").send({
            query: query,
            amount : post_amount
        })
        expect(res.body.status).toBe("ok");
    })

    it("Only call OpenSearch-Client one time with correct params", async () => {
        const res = await request(app).post("/search/posts").send({
            query: query,
            amount : post_amount
        })

        expect(os_client.search).toHaveBeenCalledTimes(1);
        expect(os_client.search.mock.calls[0][0].index).toEqual("hive-post-data");
        expect(JSON.stringify(os_client.search.mock.calls[0][0].body.query)).toMatch(new RegExp(query));
    });

    it("Correct amount of found posts", async () => {
        const res = await request(app).post("/search/posts").send({
            query: query,
            amount : post_amount
        })

        expect(os_client.search.mock.calls[0][0].body.size).toBe(post_amount);
        expect(res.body.posts.length).toBe(post_amount);
    });

    it("Posts have correct fields", async () => {
        const res = await request(app).post("/search/posts").send({
            query: query,
            amount : post_amount
        });

        for(const post of res.body.posts) {
            expect(post).toHaveProperty("author");
            expect(post).toHaveProperty("permlink");
            expect(post).toHaveProperty("score");
        }
    });

    it("Correct Sortmodes + default", async () => {
        const available_sortmodes = ["latest", "oldest", "relevance", "votes", "smart"];
        const not_available_sortmodes = ["foo", "bar"];
        const default_sortmode = "smart";

        // Check available sortmodes
        for(const sortmode of available_sortmodes) {
            const res = await request(app).post("/search/posts").send({
                query: query,
                amount : post_amount,
                sort_mode: sortmode
            });

            expect(res.body.sort_mode).toBe(sortmode);
        }

        // Check not available sortmodes to be default
        for(const sortmode of not_available_sortmodes) {
            const res = await request(app).post("/search/posts").send({
                query: query,
                amount : post_amount,
                sort_mode: sortmode
            });

            expect(res.body.sort_mode).toBe(default_sortmode);
        }
    });
});

describe("POST /search/accounts", () => {

    const [app, os_client, mongo_client] = getTestApp();

    // Reset mock function for each test
    beforeEach(() => {
        os_client.search.mockClear();
    });

    it("Fail when no query is provided with code 1", async () => {
        const res = await request(app).post("/search/accounts");
        expect(res.body.status).toBe("failed");
        expect(res.body.code).toBe(1);
    });

    const post_amount = 10
    const query = "chris"
    os_client.search.mockReturnValue(Promise.resolve({
        statusCode : 200,
        body : {
            hits : {
                total : {value : post_amount},
                hits : [...Array(post_amount).fill({_source : {name : "steve", profile : {foo : "bar"}}, _score : 1})]
            }
        }
    }));

    it("status ok", async () => {
        const res = await request(app).post("/search/accounts").send({
            query: query,
            amount : post_amount
        })
        expect(res.body.status).toBe("ok");
    })

    it("Only call OpenSearch-Client one time with correct params", async () => {
        const res = await request(app).post("/search/accounts").send({
            query: query,
            amount : post_amount
        })

        expect(os_client.search).toHaveBeenCalledTimes(1);
        expect(os_client.search.mock.calls[0][0].index).toEqual("hive-accounts");
        expect(JSON.stringify(os_client.search.mock.calls[0][0].body.query)).toMatch(new RegExp(query));
    });

    it("Correct amount of found accounts", async () => {
        const res = await request(app).post("/search/accounts").send({
            query: query,
            amount : post_amount
        })

        expect(os_client.search.mock.calls[0][0].body.size).toBe(post_amount);
        expect(res.body.accounts.length).toBe(post_amount);
    });

    it("Accounts have correct fields", async () => {
        const res = await request(app).post("/search/accounts").send({
            query: query,
            amount : post_amount
        });

        for(const acc of res.body.accounts) {
            expect(acc).toHaveProperty("name");
            expect(acc).toHaveProperty("score");
            expect(acc).toHaveProperty("json_metadata");
            expect(acc.json_metadata).toHaveProperty("profile");
        }
    });

})

describe("POST /search/similar-post", () => {
    
})

describe("POST /search/similar-by-author", () => {
    
})