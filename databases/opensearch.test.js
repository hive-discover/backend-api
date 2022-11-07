
jest.mock('@opensearch-project/opensearch', () => {
    return {
        Client: class {
            options = null;
            constructor(options) {this.options = options};

            search = jest.fn(() => {
                return Promise.resolve({
                    body: {
                        hits: {
                            hits: [...Array(10).fill({"foo" : "bar"})]
                        }
                    }
                });
            });
        }
    };
})
const opensearch = require('@opensearch-project/opensearch');

const {measureNodePerformance} = require("./opensearch.js");

describe("OpenSearch", () => {

    describe("measureNodePerformance", () => {

        beforeEach(() => {
            jest.clearAllMocks();
        });

        // describe("when the node is not reachable", () => {

        //     opensearch.Client.search.mockRejectedValueOnce(new Error("Could not connect to Opensearch Node"));

        //     it("should return null if no connection could be established", async () => {
        //         const result = await measureNodePerformance("foo.bar.com");
        //         expect(result).toBeNull();
        //     });
        // });

        describe("when the node is reachable", () => {       

            it("should return a valid result", async () => {
                const result = await measureNodePerformance("localhost:8983");

                expect(result).toBeDefined();
                expect(result.length).toBe(3);

                const [os_client, host, elapsed_seconds] = result;
                expect(os_client).toBeInstanceOf(opensearch.Client);
                expect(host).toBe("localhost:8983");
                expect(elapsed_seconds).toBeDefined();
            });
        });
    })

})