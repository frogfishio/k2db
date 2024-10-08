// __tests__/db-delete-all.test.ts

import { K2DB, DatabaseConfig } from "../src/db";
import { MongoClient, Collection } from "mongodb";

// Mock the uuid module if needed
jest.mock("uuid", () => ({
  v4: jest.fn(),
}));

describe("DB Class - deleteAll()", () => {
  const dbName = "test_delete_all"; // Use a unique test database name

  // Configuration for the test database
  const config: DatabaseConfig = {
    name: dbName,
    hosts: [
      {
        host: "localhost",
        port: 27017,
      },
    ],
    // Uncomment and set these if authentication is required
    // user: 'yourUsername',
    // password: 'yourPassword',
  };

  let dbInstance: K2DB;
  const collectionName = "deleteAllTestCollection";

  // Sample documents to insert before each test
  const initialDocuments = [
    {
      _uuid: "uuid-1",
      _owner: "user1",
      name: "Document 1",
      value: 10,
      _created: 1620000000000,
      _updated: 1620000000000,
      _deleted: false, // Explicitly set to false
    },
    {
      _uuid: "uuid-2",
      _owner: "user2",
      name: "Document 2",
      value: 20,
      _created: 1620000000000,
      _updated: 1620000000000,
      // _deleted is undefined (not deleted)
    },
    {
      _uuid: "uuid-3",
      _owner: "user3",
      name: "Document 3",
      value: 30,
      _created: 1620000000000,
      _updated: 1620000000000,
      _deleted: true, // Already soft-deleted
    },
  ];

  beforeAll(async () => {
    // Initialize the DB instance and establish a connection
    dbInstance = new K2DB(config);
    await dbInstance.init();

    // Ensure the collection has a unique index on _uuid
    const collection = await (dbInstance as any).getCollection(collectionName);
    await collection.createIndex({ _uuid: 1 }, { unique: true });
  });

  afterAll(async () => {
    // Clean up: Drop the test database and close the connection
    const adminClient = new MongoClient(`mongodb://localhost:27017`);
    await adminClient.connect();
    await adminClient.db(dbName).dropDatabase();
    await adminClient.close();
    await dbInstance.release(); // Use release() to close the connection
  });

  beforeEach(async () => {
    // Insert initial documents before each test
    const collection: Collection = await (dbInstance as any).getCollection(
      collectionName
    );
    await collection.insertMany(initialDocuments);
  });

  afterEach(async () => {
    // Clean up: Drop the collection after each test to ensure isolation
    const adminClient = new MongoClient(`mongodb://localhost:27017`);
    await adminClient.connect();
    const db = adminClient.db(dbName);
    const collections = await db
      .listCollections({ name: collectionName })
      .toArray();
    if (collections.length > 0) {
      await db.dropCollection(collectionName);
    }
    await adminClient.close();
    jest.resetAllMocks(); // Reset mocks after each test
  });

  it("should soft delete all documents matching the criteria", async () => {
    const criteria = { value: { $gte: 10 } };

    // Call the updated deleteAll function
    const result = await dbInstance.deleteAll(collectionName, criteria);

    // Assertions
    expect(result).toHaveProperty("deleted", 2); // Expect 2 documents to be soft-deleted

    // Verify that the matching documents have _deleted: true
    const collection: Collection = await (dbInstance as any).getCollection(
      collectionName
    );
    const deletedDocs = await collection
      .find({ ...criteria, _deleted: true })
      .toArray();

    // Check if all the documents matching the criteria have _deleted set to true
    deletedDocs.forEach((doc) => {
      expect(doc._deleted).toBe(true);
    });

    // Verify that documents not matching the criteria remain unchanged
    const nonMatchingDocs = await collection
      .find({ value: { $lt: 10 } })
      .toArray();
    nonMatchingDocs.forEach((doc) => {
      expect(doc._deleted).not.toBe(true);
    });
  });

  it("should not modify any documents if no documents match the criteria", async () => {
    const criteria = { value: { $gt: 1000 } }; // No documents match

    // Call the updated deleteAll function
    const result = await dbInstance.deleteAll(collectionName, criteria);

    // Assertions
    expect(result).toHaveProperty("deleted", 0); // No documents should be deleted

    // Verify that no documents have _deleted: true except the pre-deleted one
    const collection: Collection = await (dbInstance as any).getCollection(
      collectionName
    );
    const allDocs = await collection.find({}).toArray();

    allDocs.forEach((doc) => {
      if (doc._uuid === "uuid-3") {
        expect(doc._deleted).toBe(true); // This document was already deleted
      } else {
        expect(doc._deleted).not.toBe(true); // Other documents should not be modified
      }
    });
  });

  it("should throw an error when using an invalid collection name", async () => {
    const invalidCollectionName = "invalid\0name";
    const criteria = { value: { $gte: 10 } };

    await expect(
      dbInstance.deleteAll(invalidCollectionName, criteria)
    ).rejects.toThrow("Collection name cannot contain null characters");
  });

  it("should handle invalid criteria gracefully", async () => {
    const invalidCriteria = { $invalidOperator: "test" };

    await expect(
      dbInstance.deleteAll(collectionName, invalidCriteria)
    ).rejects.toThrow("Error updating deleteAllTestCollection");
  });
});
