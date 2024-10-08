import { DB, DatabaseConfig } from "../src/db";
import { MongoClient, Collection } from "mongodb";
import { v4 as uuidv4 } from "uuid"; // Import uuidv4

// Mock the uuid module
jest.mock("uuid", () => ({
  v4: jest.fn(),
}));

describe("DB Class - create()", () => {
  const dbName = "test_create"; // Use a unique test database name

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

  let dbInstance: DB;
  const collectionName = "createTestCollection";

  // Sample documents to use in tests
  const sampleData = {
    name: "Sample Document",
    value: 42,
  };

  beforeAll(async () => {
    // Initialize the DB instance and establish a connection
    dbInstance = new DB(config);
    await dbInstance.init();
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
    // Reset mocks before each test
    jest.resetAllMocks();
    // Re-create the collection and unique index before each test
    const collection = await (dbInstance as any).getCollection(collectionName);
    await collection.createIndex({ _uuid: 1 }, { unique: true });
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
  });

  it("should create a new document successfully", async () => {
    const owner = "testUser";
    const data = { name: "Sample Document", value: 42 };

    // Ensure uuidv4 returns a unique UUID
    const generatedUuid = "unique-uuid-1234";
    (uuidv4 as jest.Mock).mockReturnValue(generatedUuid);

    const result = await dbInstance.create(collectionName, owner, data);

    // Validate that the result has an 'id' property
    expect(result).toHaveProperty("id");
    expect(result.id).toBe(generatedUuid);

    // Verify that the document was inserted
    const collection: Collection = await (dbInstance as any).getCollection(
      collectionName
    );
    const insertedDoc = await collection.findOne({ _uuid: result.id });

    expect(insertedDoc).toBeDefined();
    expect(insertedDoc!._uuid).toBe(result.id);
    expect(insertedDoc!._owner).toBe("testUser");
    expect(insertedDoc!.name).toBe("Sample Document");
    expect(insertedDoc!.value).toBe(42);
    expect(insertedDoc!._deleted).toBeUndefined(); // Ensure _deleted is not set
    expect(insertedDoc!._created).toBeGreaterThan(0);
    expect(insertedDoc!._updated).toBeGreaterThan(0);
  });

  it("should throw an error when required parameters are missing", async () => {
    const owner = "testUser";
    const data = null; // Invalid data

    await expect(
      dbInstance.create(collectionName, owner, data as any)
    ).rejects.toThrow("Invalid method usage, parameters not defined");
  });

  it("should throw an error when owner is not a string", async () => {
    const owner = 12345 as any; // Invalid owner type
    const data = { name: "Invalid Owner Document", value: 100 };

    await expect(
      dbInstance.create(collectionName, owner, data)
    ).rejects.toThrow("Owner must be of a string type");
  });

  it("should include _created and _updated timestamps in the document", async () => {
    const owner = "timestampUser";
    const data = { name: "Timestamp Document", value: 55 };

    const beforeCreation = Date.now();

    // Ensure uuidv4 returns a unique UUID
    const generatedUuid = "timestamp-uuid-5678";
    (uuidv4 as jest.Mock).mockReturnValue(generatedUuid);

    const result = await dbInstance.create(collectionName, owner, data);

    const afterCreation = Date.now();

    // Verify timestamps
    const collection: Collection = await (dbInstance as any).getCollection(
      collectionName
    );
    const insertedDoc = await collection.findOne({ _uuid: result.id });

    expect(insertedDoc).toBeDefined();
    expect(insertedDoc!._created).toBeGreaterThanOrEqual(beforeCreation);
    expect(insertedDoc!._created).toBeLessThanOrEqual(afterCreation);
    expect(insertedDoc!._updated).toBeGreaterThanOrEqual(beforeCreation);
    expect(insertedDoc!._updated).toBeLessThanOrEqual(afterCreation);
  });

  it("should throw an error when inserting a document with duplicate _uuid", async () => {
    const owner = "duplicateUser";
    const data = { name: "Duplicate UUID Document", value: 999 };

    const fixedUuid = "fixed-uuid-1234";
    (uuidv4 as jest.Mock).mockReturnValue(fixedUuid);

    // First insertion should succeed
    const firstResult = await dbInstance.create(collectionName, owner, data);
    expect(firstResult.id).toBe(fixedUuid);

    // Second insertion with the same UUID should fail
    await expect(
      dbInstance.create(collectionName, owner, data)
    ).rejects.toThrow("A document with _uuid fixed-uuid-1234 already exists.");
  });

  it("should throw an error when using an invalid collection name", async () => {
    const invalidCollectionName = "invalid\0name";
    const owner = "testUser";
    const data = { name: "Invalid Collection Document", value: 50 };

    // Ensure uuidv4 returns a unique UUID
    const generatedUuid = "invalid-collection-uuid-0001";
    (uuidv4 as jest.Mock).mockReturnValue(generatedUuid);

    await expect(
      dbInstance.create(invalidCollectionName, owner, data)
    ).rejects.toThrow("Collection name cannot contain null characters");
  });
});
