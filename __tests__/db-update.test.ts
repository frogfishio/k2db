import { K2DB, DatabaseConfig } from "../src/db";
import { MongoClient, Collection } from "mongodb";

// Mock the uuid module
jest.mock("uuid", () => ({
  v4: jest.fn(),
}));

describe("DB Class - update()", () => {
  const dbName = "test_update"; // Use a unique test database name

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

  let dbInstance: K2DB;
  const collectionName = "updateTestCollection";

  // Sample document to insert before each test
  const initialDocument = {
    _uuid: "uuid-1",
    _owner: "user1",
    name: "Original Document",
    value: 10,
    _created: 1620000000000,
    _updated: 1620000000000,
  };

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
    // Insert initial document before each test
    const collection: Collection = await (dbInstance as any).getCollection(
      collectionName
    );
    await collection.insertOne(initialDocument);
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

  it("should patch a document with the provided data", async () => {
    const updatedData = { value: 100 };

    const result = await dbInstance.update(
      collectionName,
      "uuid-1",
      updatedData,
      false
    ); // Patch

    // Assertions
    expect(result).toHaveProperty("updated", 1);

    // Verify the document was patched
    const collection: Collection = await (dbInstance as any).getCollection(
      collectionName
    );
    const updatedDoc = await collection.findOne({ _uuid: "uuid-1" });

    expect(updatedDoc).toBeDefined();
    expect(updatedDoc!.value).toBe(100); // Patched value
    expect(updatedDoc!.name).toBe("Original Document"); // Name remains unchanged
    expect(updatedDoc!._updated).toBeGreaterThan(initialDocument._updated); // Timestamp should be updated
  });

  it("should replace a document with new data", async () => {
    const newDocument = {
      name: "New Replaced Document",
      value: 200,
    };

    const result = await dbInstance.update(
      collectionName,
      "uuid-1",
      newDocument,
      true
    ); // Replace

    // Assertions
    expect(result).toHaveProperty("updated", 1);

    // Verify the document was replaced
    const collection: Collection = await (dbInstance as any).getCollection(
      collectionName
    );
    const replacedDoc = await collection.findOne({ _uuid: "uuid-1" });

    expect(replacedDoc).toBeDefined();
    expect(replacedDoc!.name).toBe("New Replaced Document"); // New name
    expect(replacedDoc!.value).toBe(200); // New value
    expect(replacedDoc!._uuid).toBe("uuid-1"); // _uuid should remain the same
    expect(replacedDoc!._updated).toBeGreaterThan(initialDocument._updated); // Timestamp should be updated
    expect(replacedDoc!._created).toBe(initialDocument._created); // _created should remain unchanged
  });

  it("should throw NOT_FOUND error when document does not exist", async () => {
    const updatedData = { value: 500 };

    await expect(
      dbInstance.update(collectionName, "non-existent-uuid", updatedData, false)
    ).rejects.toThrow(
      "Object in updateTestCollection with UUID non-existent-uuid not found"
    );
  });

  it("should preserve fields starting with underscore when replacing a document", async () => {
    const newDocument = {
      name: "New Replaced Document",
      value: 200,
    };

    const result = await dbInstance.update(
      collectionName,
      "uuid-1",
      newDocument,
      true
    ); // Replace

    // Assertions
    expect(result).toHaveProperty("updated", 1);

    // Verify the document was replaced, but underscore-prefixed fields were preserved
    const collection: Collection = await (dbInstance as any).getCollection(
      collectionName
    );
    const replacedDoc = await collection.findOne({ _uuid: "uuid-1" });

    expect(replacedDoc).toBeDefined();
    expect(replacedDoc!.name).toBe("New Replaced Document"); // New name
    expect(replacedDoc!.value).toBe(200); // New value
    expect(replacedDoc!._uuid).toBe("uuid-1"); // _uuid should remain the same
    expect(replacedDoc!._created).toBe(initialDocument._created); // _created should remain unchanged
    expect(replacedDoc!._updated).toBeGreaterThan(initialDocument._updated); // _updated timestamp should be updated
  });

  it("should handle invalid collection name gracefully", async () => {
    const invalidCollectionName = "invalid\0name";
    const updatedData = { value: 100 };

    await expect(
      dbInstance.update(invalidCollectionName, "uuid-1", updatedData, false)
    ).rejects.toThrow("Collection name cannot contain null characters");
  });

  it("should remove specified fields when replacing a document", async () => {
    // Initial document setup with additional fields
    const initialDocument = {
      _uuid: "uuid-1",
      _owner: "user1",
      name: "Original Document",
      value: 10,
      c: "valueC",
      e: "valueE",
      g: "valueG",
      _created: 1620000000000,
      _updated: 1620000000000,
    };

    // Insert the initial document
    const collection: Collection = await (dbInstance as any).getCollection(
      collectionName
    );
    await collection.insertOne(initialDocument);

    // New document excluding fields 'c' and 'e'
    const newDocument = {
      name: "New Replaced Document",
      value: 200,
      g: "newValueG", // keep this field
    };

    const result = await dbInstance.update(
      collectionName,
      "uuid-1",
      newDocument,
      true // Use replace mode
    ); // Replace

    // Assertions
    expect(result).toHaveProperty("updated", 1);

    // Verify the document was replaced and fields 'c' and 'e' are removed
    const updatedDoc = await collection.findOne({ _uuid: "uuid-1" });

    expect(updatedDoc).toBeDefined();
    expect(updatedDoc!.name).toBe("New Replaced Document"); // New name
    expect(updatedDoc!.value).toBe(200); // New value
    expect(updatedDoc!).not.toHaveProperty("c"); // Field 'c' should be removed
    expect(updatedDoc!).not.toHaveProperty("e"); // Field 'e' should be removed
    expect(updatedDoc!.g).toBe("newValueG"); // 'g' should remain and be updated
    expect(updatedDoc!._uuid).toBe("uuid-1"); // _uuid should remain the same
    expect(updatedDoc!._created).toBe(initialDocument._created); // _created should remain unchanged
    expect(updatedDoc!._updated).toBeGreaterThan(initialDocument._updated); // _updated should be updated
  });
});
