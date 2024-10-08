// __tests__/db.test.ts

import { DB, DatabaseConfig } from "../src/db";
import { MongoClient, Collection } from "mongodb";

// Mock the uuid module
jest.mock("uuid", () => ({
  v4: jest.fn(),
}));

describe("DB Class", () => {
  const dbName = "test"; // Use the database name that works

  // Configuration that works
  const config: DatabaseConfig = {
    name: dbName,
    hosts: [
      {
        host: "localhost",
        port: 27017,
      },
    ],
    // Uncomment and set these if your MongoDB instance requires authentication
    // user: 'yourUsername',
    // password: 'yourPassword',
  };

  let dbInstance: DB;

  beforeAll(async () => {
    // Initialize the DB instance and establish a connection
    dbInstance = new DB(config);
    await dbInstance.init();

    // Ensure the testCollection has a unique index on _uuid
    const collection = await (dbInstance as any).getCollection(
      "testCollection"
    );
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

  describe("Initialization", () => {
    it("should initialize MongoDB connection with a single host", async () => {
      // Use isHealthy() to check connection
      const isHealthy = await dbInstance.isHealthy();
      expect(isHealthy).toBe(true);
    });

    it("should throw an error if MongoDB connection fails", async () => {
      // Create a DB instance with an incorrect port to simulate connection failure
      const badConfig: DatabaseConfig = {
        name: dbName,
        hosts: [
          {
            host: "localhost",
            port: 27018, // Assuming nothing is running on this port
          },
        ],
        // Uncomment and set these if authentication is required
        // user: 'invalidUser',
        // password: 'invalidPassword',
      };
      const badDbInstance = new DB(badConfig);

      await expect(badDbInstance.init()).rejects.toThrow(
        "Failed to connect to MongoDB"
      );
    }, 10000); // Set timeout to 10 seconds
  });

  describe("getCollection()", () => {
    it("should retrieve a collection successfully", async () => {
      // Access the private getCollection method using bracket notation
      const collectionName = "testCollection";
      const collection: Collection = await (dbInstance as any).getCollection(
        collectionName
      );

      // Validate that the collection is defined
      expect(collection).toBeDefined();

      // Validate that the collection name matches
      expect(collection.collectionName).toBe(collectionName);

      // Optionally, perform a simple operation to ensure the collection is functional
      const count = await collection.countDocuments();
      expect(typeof count).toBe("number");
    });

    it("should throw an error when retrieving an invalid collection", async () => {
      // Attempting to retrieve a collection with an invalid name format
      const invalidCollectionName = "invalid\0name";

      await expect(
        (dbInstance as any).getCollection(invalidCollectionName)
      ).rejects.toThrow("Collection name cannot contain null characters");
    });
  });
});
