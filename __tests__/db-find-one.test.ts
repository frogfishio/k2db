import { K2DB, DatabaseConfig } from "../src/db";
import { v4 as uuidv4 } from "uuid"; // Import uuidv4

// Mock the uuid module
jest.mock("uuid", () => ({
  v4: jest.fn(),
}));

describe("DB Class - findOne()", () => {
  const dbName = "test_find_one"; // Use a unique test database name

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
  const collectionName = "findOneTestCollection";

  // Sample document to insert before each test
  const testDocument = {
    _uuid: "findone-uuid-5678",
    _owner: "findOneUser",
    name: "FindOne Test Document",
    value: 100,
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
    await dbInstance.dropDatabase();
    await dbInstance.release(); // Use release() to close the connection
  });

  beforeEach(async () => {
    // Mock uuidv4 to return a fixed UUID
    (uuidv4 as jest.Mock).mockReturnValue(testDocument._uuid);

    // Insert the test document
    await dbInstance.create(collectionName, testDocument._owner, {
      name: testDocument.name,
      value: testDocument.value,
    });
  });

  afterEach(async () => {
    // Clean up: Delete all documents after each test
    const collection = await (dbInstance as any).getCollection(collectionName);
    await collection.deleteMany({});
    jest.resetAllMocks(); // Reset mocks after each test
  });

  it("should retrieve an existing document successfully", async () => {
    const criteria = { _uuid: testDocument._uuid };

    const retrievedDoc = await dbInstance.findOne(collectionName, criteria);

    expect(retrievedDoc).not.toBeNull(); // Ensure retrievedDoc is not null

    // Use the non-null assertion operator (!) or TypeScript will complain
    expect(retrievedDoc!._uuid).toBe(testDocument._uuid);
    expect(retrievedDoc!._owner).toBe(testDocument._owner);
    expect(retrievedDoc!.name).toBe(testDocument.name);
    expect(retrievedDoc!.value).toBe(testDocument.value);
    expect(retrievedDoc!._deleted).toBeUndefined(); // Ensure _deleted is not set
  });

  it("should return null when the document does not exist", async () => {
    const nonExistentUuid = "non-existent-uuid-0000";
    const criteria = { _uuid: nonExistentUuid };

    const retrievedDoc = await dbInstance.findOne(collectionName, criteria);

    expect(retrievedDoc).toBeNull();
  });

  it("should retrieve a document with specific fields included", async () => {
    const criteria = { _uuid: testDocument._uuid };
    const fields = ["_uuid", "name", "value"];

    const retrievedDoc = await dbInstance.findOne(
      collectionName,
      criteria,
      fields
    );

    expect(retrievedDoc).not.toBeNull(); // Ensure retrievedDoc is not null

    expect(retrievedDoc!._uuid).toBe(testDocument._uuid);
    expect(retrievedDoc!.name).toBe(testDocument.name);
    expect(retrievedDoc!.value).toBe(testDocument.value);

    // Fields not included should be undefined
    expect((retrievedDoc as any)._owner).toBeUndefined();
    expect((retrievedDoc as any)._created).toBeUndefined();
    expect((retrievedDoc as any)._updated).toBeUndefined();
    expect((retrievedDoc as any)._deleted).toBeUndefined();
  });

  it("should retrieve a document with specific fields excluded", async () => {
    const criteria = { _uuid: testDocument._uuid };
    const fieldsToExclude = ["_owner", "_created", "_updated"];

    const fields = ["_uuid", "name", "value"];

    const retrievedDoc = await dbInstance.findOne(
      collectionName,
      criteria,
      fields
    );

    expect(retrievedDoc).not.toBeNull(); // Ensure retrievedDoc is not null

    expect(retrievedDoc!._uuid).toBe(testDocument._uuid);

    // Fields excluded should be undefined
    expect((retrievedDoc as any)._owner).toBeUndefined();
    expect((retrievedDoc as any)._created).toBeUndefined();
    expect((retrievedDoc as any)._updated).toBeUndefined();

    // Included fields should be present
    expect(retrievedDoc!.name).toBe(testDocument.name);
    expect(retrievedDoc!.value).toBe(testDocument.value);
    expect(retrievedDoc!._deleted).toBeUndefined();
  });

  it("should throw an error when using an invalid collection name", async () => {
    const invalidCollectionName = "invalid\0name";
    const criteria = { _uuid: testDocument._uuid };

    await expect(
      dbInstance.findOne(invalidCollectionName, criteria)
    ).rejects.toThrow("Collection name cannot contain null characters");
  });

  it("should handle invalid criteria gracefully", async () => {
    const invalidCriteria = { $invalidOperator: "test" };

    await expect(
      dbInstance.findOne(collectionName, invalidCriteria)
    ).rejects.toThrow("Error finding document");
  });
});
