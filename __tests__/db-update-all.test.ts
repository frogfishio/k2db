// __tests__/db-update-all.test.ts

import { K2DB, DatabaseConfig } from "../src/db";
import { MongoClient, Collection } from "mongodb";

// Mock the uuid module
jest.mock("uuid", () => ({
  v4: jest.fn(),
}));

describe("DB Class - updateAll()", () => {
  const dbName = "test_update_all"; // Use a unique test database name

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
  const collectionName = "updateAllTestCollection";

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

  it("should update multiple documents successfully based on criteria", async () => {
    const criteria = { value: { $gte: 10 } };
    const values = { value: 100 };

    const result = await dbInstance.updateAll(collectionName, criteria, values);

    // Assertions
    expect(result).toHaveProperty("updated", 2); // Should be 2 after excluding soft-deleted documents

    // Verify that the documents were updated
    const collection: Collection = await (dbInstance as any).getCollection(
      collectionName
    );
    const updatedDocs = await collection.find({ value: 100 }).toArray();

    expect(updatedDocs.length).toBe(2);
    updatedDocs.forEach((doc) => {
      expect(doc.value).toBe(100);
      expect(doc._updated).toBeGreaterThan(initialDocuments[0]._updated);
    });
  });

  it("should not modify any documents if criteria does not match", async () => {
    const criteria = { value: { $gt: 1000 } }; // No documents match
    const values = { value: 2000 };

    const result = await dbInstance.updateAll(collectionName, criteria, values);

    // Assertions
    expect(result).toHaveProperty("updated", 0); // No documents should be updated

    // Verify that no documents were updated
    const collection: Collection = await (dbInstance as any).getCollection(
      collectionName
    );
    const allDocs = await collection.find({}).toArray();

    allDocs.forEach((doc) => {
      if (!doc._deleted) {
        expect(doc.value).not.toBe(2000);
        expect(doc._updated).toBe(
          initialDocuments.find((d) => d._uuid === doc._uuid)!._updated
        );
      }
    });
  });

  it("should update only specified fields and _updated timestamp", async () => {
    const criteria = { _owner: "user1" };
    const values = { name: "Updated Document 1" };

    const beforeUpdate = Date.now();

    const result = await dbInstance.updateAll(collectionName, criteria, values);

    // Assertions
    expect(result).toHaveProperty("updated", 1); // One document should be updated

    // Verify that the specific field was updated
    const collection: Collection = await (dbInstance as any).getCollection(
      collectionName
    );
    const updatedDoc = await collection.findOne({ _uuid: "uuid-1" });

    expect(updatedDoc).toBeDefined();
    expect(updatedDoc!.name).toBe("Updated Document 1");
    expect(updatedDoc!.value).toBe(10); // Remains unchanged as we didn't update it
    expect(updatedDoc!._updated).toBeGreaterThanOrEqual(beforeUpdate);
  });

  it("should throw an error when updating with invalid collection name", async () => {
    const invalidCollectionName = "invalid\0name";
    const criteria = { _owner: "user1" };
    const values = { value: 500 };

    await expect(
      dbInstance.updateAll(invalidCollectionName, criteria, values)
    ).rejects.toThrow("Collection name cannot contain null characters");
  });

  it("should handle invalid update values gracefully", async () => {
    const criteria = { _owner: "user2" };
    const values = { $invalidOperator: "test" }; // Invalid MongoDB operator

    await expect(
      dbInstance.updateAll(collectionName, criteria, values)
    ).rejects.toThrow("Error updating updateAllTestCollection");
  });

  it("should update the _updated timestamp correctly", async () => {
    const criteria = { _owner: "user3", _deleted: true }; // Include _deleted: true
    const values = { name: "Revived Document" };

    const beforeUpdate = Date.now();

    const result = await dbInstance.updateAll(collectionName, criteria, values);

    // Assertions
    expect(result).toHaveProperty("updated", 1); // One document should be updated

    // Verify that the _updated timestamp was updated
    const collection: Collection = await (dbInstance as any).getCollection(
      collectionName
    );
    const updatedDoc = await collection.findOne({ _uuid: "uuid-3" });

    expect(updatedDoc).toBeDefined();
    expect(updatedDoc!.name).toBe("Revived Document");
    expect(updatedDoc!._updated).toBeGreaterThanOrEqual(beforeUpdate);
  });
});
