import { DB, DatabaseConfig } from "../src/db";
import { Collection } from "mongodb";

describe("DB Class - find()", () => {
  const dbName = "test_find"; // Use a unique test database name

  // Configuration for the test database
  const config: DatabaseConfig = {
    name: dbName,
    hosts: [
      {
        host: "localhost",
        port: 27017,
      },
    ],
  };

  let dbInstance: DB;
  const collectionName = "findTestCollection";

  // Sample documents to insert before each test
  const initialDocuments = [
    {
      _uuid: "uuid-1",
      _owner: "user1",
      name: "Document 1",
      value: 10,
      _created: 1620000000000,
      _updated: 1620000000000,
    },
    {
      _uuid: "uuid-2",
      _owner: "user2",
      name: "Document 2",
      value: 20,
      _created: 1620000000000,
      _updated: 1620000000000,
    },
    {
      _uuid: "uuid-3",
      _owner: "user3",
      name: "Document 3",
      value: 30,
      _created: 1620000000000,
      _updated: 1620000000000,
      _deleted: true, // Soft-deleted document
    },
    {
      _uuid: "uuid-4",
      _owner: "user4",
      name: "Document 4",
      value: 40,
      _created: 1620000000000,
      _updated: 1620000000000,
    },
  ];

  beforeAll(async () => {
    dbInstance = new DB(config);
    await dbInstance.init();

    const collection = await (dbInstance as any).getCollection(collectionName);
    await collection.createIndex({ _uuid: 1 }, { unique: true });
  });

  afterAll(async () => {
    await dbInstance.dropDatabase();
    await dbInstance.release(); // Close the connection
  });

  beforeEach(async () => {
    const collection: Collection = await (dbInstance as any).getCollection(
      collectionName
    );

    await collection.deleteMany({}); // Clear previous data
    await collection.insertMany(initialDocuments); // Insert test documents
  });

  afterEach(async () => {
    jest.resetAllMocks(); // Reset mocks after each test
  });

  it("should retrieve all non-deleted documents when no criteria is provided", async () => {
    const result = await dbInstance.find(collectionName, {}, {});

    expect(Array.isArray(result)).toBe(true);
    expect(result.length).toBe(3); // Excludes the soft-deleted document
  });

  it("should retrieve documents matching the filter criteria", async () => {
    const filter = { value: { $gte: 20 } };
    const result = await dbInstance.find(collectionName, filter);

    expect(Array.isArray(result)).toBe(true);
    expect(result.length).toBe(2); // Documents with value >= 20 and not deleted
    expect(result[0].value).toBeGreaterThanOrEqual(20);
  });

  it("should include only specified fields", async () => {
    const filter = { _uuid: "uuid-1" };
    const params = { filter: ["name", "value"] };
    const result = await dbInstance.find(collectionName, filter, params);

    expect(result.length).toBe(1);
    const doc = result[0];
    expect(doc).toHaveProperty("name", "Document 1");
    expect(doc).toHaveProperty("value", 10);
    expect(doc).not.toHaveProperty("_owner");
  });

  it("should exclude specified fields", async () => {
    const filter = { _uuid: "uuid-2" };
    const params = { exclude: ["name", "value"] };
    const result = await dbInstance.find(collectionName, filter, params);

    expect(result.length).toBe(1);
    const doc = result[0];
    expect(doc).toHaveProperty("_uuid", "uuid-2");
    expect(doc).not.toHaveProperty("name");
    expect(doc).not.toHaveProperty("value");
  });

  it("should apply sorting correctly", async () => {
    const params = { order: { value: "desc" } };
    const result = await dbInstance.find(collectionName, {}, params);

    expect(result.length).toBe(3);
    expect(result[0].value).toBeGreaterThan(result[1].value);
  });

  it("should apply pagination with skip and limit", async () => {
    const params = { order: { value: "asc" } };
    const result = await dbInstance.find(collectionName, {}, params, 1, 1);

    expect(result.length).toBe(1);
    expect(result[0].value).toBe(20); // Skipped the first and limited to one result
  });

  it("should include soft-deleted documents when deleted parameter is true", async () => {
    const params = { deleted: true };
    const result = await dbInstance.find(collectionName, {}, params);

    expect(result.length).toBe(1);
    expect(result[0]._deleted).toBe(true);
  });

  it("should include all fields when filter is set to 'all'", async () => {
    const filter = { _uuid: "uuid-1" };
    const params = { filter: "all" };
    const result = await dbInstance.find(collectionName, filter, params);

    const doc = result[0];
    expect(doc).toHaveProperty("_uuid", "uuid-1");
    expect(doc).toHaveProperty("name", "Document 1");
    expect(doc).toHaveProperty("_owner", "user1");
  });

  it("should include all documents when includeDeleted is true", async () => {
    const params = { includeDeleted: true };
    const result = await dbInstance.find(collectionName, {}, params);

    expect(result.length).toBe(4); // Includes the soft-deleted document
  });

  it("should throw an error when using an invalid collection name", async () => {
    const invalidCollectionName = "invalid\0name";
    const filter = { value: { $gte: 10 } };

    await expect(
      dbInstance.find(invalidCollectionName, filter)
    ).rejects.toThrow("Collection name cannot contain null characters");
  });

  it("should handle invalid criteria gracefully", async () => {
    const filter = { $invalidOperator: "test" };

    await expect(dbInstance.find(collectionName, filter)).rejects.toThrow(
      "Error executing find query"
    );
  });

  it("should return an empty array when no documents match the filter", async () => {
    const filter = { value: { $gt: 1000 } };
    const result = await dbInstance.find(collectionName, filter);

    expect(result.length).toBe(0);
  });
});
