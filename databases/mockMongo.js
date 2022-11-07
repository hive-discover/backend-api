
const mocks = {
    toArray : jest.fn(() => Promise.resolve([])),
    aggregate : jest.fn(() => ({toArray : mocks.toArray})),
    find : jest.fn(() => ({toArray : mocks.toArray})),
    collection : jest.fn(() => ({aggregate : mocks.aggregate, find : mocks.find})),
    db : jest.fn(() => ({collection : mocks.collection}))
}

module.exports = {     
    ...mocks,

    mockClear : () => {
        mocks.db.mockClear();
        mocks.collection.mockClear();
        mocks.aggregate.mockClear();
        mocks.find.mockClear();
        mocks.toArray.mockClear();
    }
}