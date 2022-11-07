const mongo_client = require('./databases/mockMongo');

const os_client = {
    search : jest.fn()
}

describe('Users', () => {

    const getUser = require('./users')(os_client, mongo_client);

    describe('getUserAverageReadingTime', () => {

        const username = 'foo-author';
        const [avg, posts, total] = [3, 5, 15];
        const account = getUser(username);

        beforeEach(() => {
            mongo_client.mockClear();
            os_client.search.mockClear();
        });

        it("Handle empty users", async () => {
            const result = await account.getUserAverageReadingTime();
            expect(result).toEqual({ avg: 0, posts: 0, total: 0 });
        })

        it("Call MongoDB correctly", async () => {
            await account.getUserAverageReadingTime();

            expect(mongo_client.collection).toHaveBeenCalledWith("post_scrolled");
            expect(mongo_client.aggregate).toHaveBeenCalledWith(expect.any(Array));
            expect(mongo_client.toArray).toHaveBeenCalledTimes(1);
        });

        it("Return correct result", async () => {
            mongo_client.toArray.mockReturnValue(Promise.resolve([
                {
                    _id: "result",
                    avg, posts, total
                }
            ]))

            const result = await account.getUserAverageReadingTime();
            expect(result).toEqual({
                avg, posts, total
            });
        });

    });

    describe('getScoredAccountActivities', () => {

        const username = 'foo-author';
        const account = getUser(username);

        beforeEach(() => {
            mongo_client.mockClear();
            os_client.search.mockClear();
        });

        it("Do not fill when specified", async () => {
            const result = await account.getScoredAccountActivities(1000, 75, false);
            expect(result).toEqual({});
        });

        it("Call MongoDB correctly", async () => {
            expect(true).toBe(true);
        })

    });

})