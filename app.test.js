const request = require("supertest");

const makeApp = require("./app.js");

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
const app = makeApp(null, mongo_client);


describe("Index Route", () => {
    it("should return 200", async () => {
        const response = await request(app).get("/");
        expect(response.status).toBe(200);
    });

    it("should return a status-ok as a json object", async () => {
        const response = await request(app).get("/");
        expect(response.type).toBe("application/json");
        expect(response.body.status).toBe("ok");
    });
});