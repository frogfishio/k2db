// src/db.ts

import { K2Error, ServiceError } from "@frogfish/k2error"; // Keep the existing error structure
import {
  MongoClient,
  Db,
  Collection,
  MongoClientOptions,
  ObjectId,
  Filter,
  OptionalUnlessRequiredId,
} from "mongodb";
import { v4 as uuidv4 } from "uuid";
import debugLib from "debug";

const debug = debugLib("k2:db");

export interface HostConfig {
  host: string; // host name or IP
  port?: number; // @default 27017
}

export interface DatabaseConfig {
  name: string; // db name
  user?: string; // username
  password?: string; // password
  hosts?: HostConfig[];
  replicaset?: string; // required if more than one host
}

export interface BaseDocument {
  _id?: ObjectId; // Optional because it might not be present before insertion
  _uuid: string;
  _created: number;
  _updated: number;
  _owner: string;
  _deleted?: boolean; // Ensure _deleted is optional and a boolean
  [key: string]: any; // Allow additional properties
}

export class K2DB {
  private db!: Db;
  private connection!: MongoClient;

  constructor(private conf: DatabaseConfig) {}

  /**
   * Initializes the MongoDB connection.
   */
  async init(): Promise<void> {
    const dbName = this.conf.name;
    let connectUrl = "mongodb://";

    // Add user and password if available
    if (this.conf.user && this.conf.password) {
      connectUrl += `${encodeURIComponent(this.conf.user)}:${encodeURIComponent(
        this.conf.password
      )}@`;
    }

    // Handle single host (non-replicaset) or multiple hosts (replicaset)
    if (!this.conf.hosts || this.conf.hosts.length === 0) {
      throw new K2Error(
        ServiceError.CONFIGURATION_ERROR,
        "No valid hosts provided in configuration",
        "sys_mdb_no_hosts"
      );
    }

    connectUrl += this.conf.hosts
      .map((host) => `${host.host}:${host.port || 27017}`)
      .join(",");

    // Append database name
    connectUrl += `/${dbName}`;

    // Append replicaset and options if it's a replicaset
    if (this.conf.hosts.length > 1 && this.conf.replicaset) {
      connectUrl += `?replicaSet=${this.conf.replicaset}&keepAlive=true&autoReconnect=true&socketTimeoutMS=0`;
    }

    // Mask sensitive information in logs
    const safeConnectUrl = connectUrl.replace(/\/\/.*?:.*?@/, "//*****:*****@");
    debug(`Connecting to MongoDB: ${safeConnectUrl}`);

    // Define connection options with timeouts
    const options: MongoClientOptions = {
      connectTimeoutMS: 2000, // 2 seconds
      serverSelectionTimeoutMS: 2000, // 2 seconds
      // Additional options can be added here
    };

    try {
      // Establish MongoDB connection
      this.connection = await MongoClient.connect(connectUrl, options);
      this.db = this.connection.db(dbName);
      debug("Successfully connected to MongoDB");
    } catch (err) {
      // Handle connection error
      throw new K2Error(
        ServiceError.SYSTEM_ERROR,
        "Failed to connect to MongoDB",
        "sys_mdb_init",
        this.normalizeError(err)
      );
    }
  }

  /**
   * Retrieves a collection from the database.
   * @param collectionName - Name of the collection.
   */
  private async getCollection(
    collectionName: string
  ): Promise<Collection<BaseDocument>> {
    try {
      this.validateCollectionName(collectionName); // Validate the collection name

      const collection = this.db.collection<BaseDocument>(collectionName);
      return collection;
    } catch (err) {
      // If the error is already an K2Error, rethrow it
      if (err instanceof K2Error) {
        throw err;
      }

      throw new K2Error(
        ServiceError.SYSTEM_ERROR,
        `Error getting collection: ${collectionName}`,
        "sys_mdb_gc",
        this.normalizeError(err)
      );
    }
  }

  async get(collectionName: string, uuid: string): Promise<BaseDocument> {
    const res = await this.findOne(collectionName, { _uuid: uuid });

    if (!res) {
      throw new K2Error(
        ServiceError.SYSTEM_ERROR,
        "Error getting the document with provided identity",
        "sys_mdb_get"
      );
    }

    return res;
  }

  /**
   * Retrieves a single document by UUID.
   * @param collectionName - Name of the collection.
   * @param uuid - UUID of the document.
   * @param objectTypeName - Optional object type name.
   * @param fields - Optional array of fields to include.
   */
  async findOne(
    collectionName: string,
    criteria: any,
    fields?: Array<string>
  ): Promise<BaseDocument | null> {
    const collection = await this.getCollection(collectionName);
    const projection: any = {};

    if (fields && fields.length > 0) {
      fields.forEach((field) => {
        projection[field] = 1;
      });
    }

    try {
      const item = await collection.findOne(criteria, { projection });
      if (item) {
        const { _id, ...rest } = item;
        return rest as BaseDocument;
      }
      return null;
    } catch (err) {
      throw new K2Error(
        ServiceError.SYSTEM_ERROR,
        "Error finding document",
        "sys_mdb_fo",
        this.normalizeError(err)
      );
    }
  }

  /**
   * Finds documents based on parameters with pagination support.
   * @param collectionName - Name of the collection.
   * @param filter - Criteria to filter the documents.
   * @param params - Optional search parameters (for sorting, including/excluding fields).
   * @param skip - Number of documents to skip (for pagination).
   * @param limit - Maximum number of documents to return.
   */
  async find(
    collectionName: string,
    filter: any,
    params: any = {},
    skip: number = 0,
    limit: number = 100
  ): Promise<BaseDocument[]> {
    const collection = await this.getCollection(collectionName);

    // Ensure filter is valid, defaulting to an empty object
    const criteria = filter || {};

    // Handle the _deleted field if params specify not to include deleted documents
    if (params.includeDeleted) {
      // No _deleted filter, include all documents
    } else if (params.deleted === true) {
      criteria._deleted = true; // Explicitly search for deleted documents
    } else {
      criteria._deleted = { $ne: true }; // Exclude deleted by default
    }

    // Build projection (fields to include or exclude)
    let projection: any = { _id: 0 }; // Exclude _id by default

    if (typeof params.filter === "string" && params.filter === "all") {
      projection = {}; // Include all fields
    } else if (Array.isArray(params.filter)) {
      params.filter.forEach((field: string) => {
        projection[field] = 1; // Only include the specified fields
      });
    }

    if (Array.isArray(params.exclude)) {
      params.exclude.forEach((field: string) => {
        projection[field] = 0; // Exclude the specified fields
      });
    }

    // Build sorting options
    let sort: any = undefined;
    if (params.order) {
      sort = {};
      for (const [key, value] of Object.entries(params.order)) {
        sort[key] = value === "asc" ? 1 : -1;
      }
    }

    try {
      let cursor = collection.find(criteria, { projection });

      // Apply pagination
      cursor = cursor.skip(skip).limit(limit);

      if (sort) {
        cursor = cursor.sort(sort);
      }

      const data = await cursor.toArray();

      // Remove _id safely from each document
      const result = data.map((doc) => {
        const { _id, ...rest } = doc;
        return rest as BaseDocument;
      });

      return result;
    } catch (err) {
      throw new K2Error(
        ServiceError.SYSTEM_ERROR,
        "Error executing find query",
        "sys_mdb_find_error",
        this.normalizeError(err)
      );
    }
  }

  /**
   * Aggregates documents based on criteria with pagination support.
   * @param collectionName - Name of the collection.
   * @param criteria - Aggregation pipeline criteria.
   * @param skip - Number of documents to skip (for pagination).
   * @param limit - Maximum number of documents to return.
   */
  async aggregate(
    collectionName: string,
    criteria: any[],
    skip: number = 0,
    limit: number = 100
  ): Promise<BaseDocument[]> {
    if (criteria.length === 0) {
      throw new K2Error(
        ServiceError.SYSTEM_ERROR,
        "Aggregation criteria cannot be empty",
        "sys_mdb_ag_empty"
      );
    }

    // Ensure we always exclude soft-deleted documents
    if (criteria[0].$match) {
      criteria[0].$match = { ...criteria[0].$match, _deleted: { $ne: true } };
    } else {
      criteria.unshift({ $match: { _deleted: { $ne: true } } });
    }

    // Add pagination stages to the aggregation pipeline
    if (skip > 0) {
      criteria.push({ $skip: skip });
    }

    if (limit > 0) {
      criteria.push({ $limit: limit });
    }

    debug(`Aggregating with criteria: ${JSON.stringify(criteria, null, 2)}`);

    const collection = await this.getCollection(collectionName);

    // Sanitize criteria
    const sanitizedCriteria = criteria.map((stage) => {
      if (stage.$match) {
        return K2DB.sanitiseCriteria(stage);
      }
      return stage;
    });

    try {
      const data = await collection.aggregate(sanitizedCriteria).toArray();
      // Enforce BaseDocument type on each document
      return data.map((doc) => doc as BaseDocument);
    } catch (err) {
      throw new K2Error(
        ServiceError.SYSTEM_ERROR,
        "Aggregation failed",
        "sys_mdb_ag",
        this.normalizeError(err)
      );
    }
  }

  /**
   * Creates a new document in the collection.
   * @param collectionName - Name of the collection.
   * @param owner - Owner of the document.
   * @param data - Data to insert.
   */
  async create(
    collectionName: string,
    owner: string,
    data: Partial<BaseDocument>
  ): Promise<{ id: string }> {
    if (!collectionName || !owner || !data) {
      throw new K2Error(
        ServiceError.BAD_REQUEST,
        "Invalid method usage, parameters not defined",
        "sys_mdb_crv1"
      );
    }

    if (typeof owner !== "string") {
      throw new K2Error(
        ServiceError.BAD_REQUEST,
        "Owner must be of a string type",
        "sys_mdb_crv2"
      );
    }

    const collection = await this.getCollection(collectionName);

    const timestamp = Date.now();
    // Generate a new UUID
    const newUuid = uuidv4();

    // Spread `data` first, then set internal fields to prevent overwriting
    const document: BaseDocument = {
      ...data,
      _created: timestamp,
      _updated: timestamp,
      _owner: owner,
      _uuid: newUuid,
    };

    try {
      const result = await collection.insertOne(
        document as OptionalUnlessRequiredId<BaseDocument>
      );

      return { id: document._uuid };
    } catch (err: any) {
      // Use appropriate error typing
      // Check if the error is a duplicate key error
      if (err.code === 11000 && err.keyPattern && err.keyPattern._uuid) {
        throw new K2Error(
          ServiceError.ALREADY_EXISTS,
          `A document with _uuid ${document._uuid} already exists.`,
          "sys_mdb_crv3"
        );
      }

      // Log the error details for debugging
      debug(
        `Was trying to insert into collection ${collectionName}, data: ${JSON.stringify(
          document
        )}`
      );
      debug(err);

      throw new K2Error(
        ServiceError.SYSTEM_ERROR,
        "Error saving object to database",
        "sys_mdb_sav",
        this.normalizeError(err)
      );
    }
  }

  /**
   * Updates multiple documents based on criteria.
   * Can either replace the documents or patch them.
   * @param collectionName - Name of the collection.
   * @param criteria - Update criteria.
   * @param values - Values to update or replace with.
   * @param replace - If true, replaces the entire document (PUT), otherwise patches (PATCH).
   */
  async updateAll(
    collectionName: string,
    criteria: any,
    values: Partial<BaseDocument>,
    replace: boolean = false
  ): Promise<{ updated: number }> {
    // Return type changed to JSON object {updated: number}
    const collection = await this.getCollection(collectionName);
    debug(
      `Updating ${collectionName} with criteria: ${JSON.stringify(criteria)}`
    );

    values._updated = Date.now();

    // Exclude soft-deleted documents unless _deleted is explicitly specified in criteria
    if (!("_deleted" in criteria)) {
      criteria = {
        ...criteria,
        _deleted: { $ne: true },
      };
    }

    // Determine update operation based on the replace flag
    const updateOperation = replace ? values : { $set: values };

    try {
      const res = await collection.updateMany(criteria, updateOperation);
      // Return the updated count in JSON format
      return { updated: res.modifiedCount };
    } catch (err) {
      throw new K2Error(
        ServiceError.SYSTEM_ERROR,
        `Error updating ${collectionName}`,
        "sys_mdb_update1",
        this.normalizeError(err)
      );
    }
  }

  /**
   * Updates a single document by UUID.
   * Can either replace the document or patch it.
   * @param collectionName - Name of the collection.
   * @param id - UUID string to identify the document.
   * @param data - Data to update or replace with.
   * @param replace - If true, replaces the entire document (PUT), otherwise patches (PATCH).
   * @param objectTypeName - Optional object type name.
   */
  async update(
    collectionName: string,
    id: string,
    data: Partial<BaseDocument>,
    replace: boolean = false,
    objectTypeName?: string
  ): Promise<{ updated: number }> {
    const collection = await this.getCollection(collectionName);
    data._updated = Date.now(); // Set the _updated timestamp

    try {
      // Call updateAll with the UUID criteria and replace flag
      const res = await this.updateAll(
        collectionName,
        { _uuid: id } as Filter<BaseDocument>,
        data,
        replace
      );

      // Check if exactly one document was updated
      if (res.updated === 1) {
        return { updated: 1 };
      }

      // If no document was updated, throw a NOT_FOUND error
      if (res.updated === 0) {
        throw new K2Error(
          ServiceError.NOT_FOUND,
          `Object in ${collectionName} with UUID ${id} not found`,
          "sys_mdb_update_not_found"
        );
      }

      // If more than one document was updated, throw a SYSTEM_ERROR
      if (res.updated > 1) {
        throw new K2Error(
          ServiceError.SYSTEM_ERROR,
          `Multiple objects in ${collectionName} were updated when only one was expected`,
          "sys_mdb_update_multiple_found"
        );
      }

      // Since we've handled all possible valid cases, we return undefined in case of unforeseen errors (for type safety)
      return { updated: 0 }; // Should never reach this line, here for type safety
    } catch (err) {
      if (err instanceof K2Error) {
        throw err;
      }
      // Catch any other unhandled errors and throw a system error
      throw new K2Error(
        ServiceError.SYSTEM_ERROR,
        `Error updating ${collectionName}`,
        "sys_mdb_update_error",
        this.normalizeError(err)
      );
    }
  }

  /**
   * Removes (soft deletes) multiple documents based on criteria.
   * @param collectionName - Name of the collection.
   * @param criteria - Removal criteria.
   */
  async deleteAll(
    collectionName: string,
    criteria: any
  ): Promise<{ deleted: number }> {
    const collection = await this.getCollection(collectionName);

    try {
      // Step 1: Count documents matching the original criteria (this is not strictly necessary if you only want the deleted count)
      const foundCount = await collection.countDocuments(criteria);
    } catch (err) {
      throw new K2Error(
        ServiceError.SYSTEM_ERROR,
        `Error updating ${collectionName}`,
        "sys_mdb_deleteall_count",
        this.normalizeError(err)
      );
    }

    // Step 2: Modify criteria to exclude already deleted documents
    const modifiedCriteria = {
      ...criteria,
      _deleted: { $ne: true },
    };

    let result;

    try {
      // Perform the update to soft delete the documents
      result = await this.updateAll(collectionName, modifiedCriteria, {
        _deleted: true,
      } as Partial<BaseDocument>);
    } catch (err) {
      throw new K2Error(
        ServiceError.SYSTEM_ERROR,
        `Error updating ${collectionName}`,
        "sys_mdb_deleteall_update",
        this.normalizeError(err)
      );
    }

    // Step 3: Return the number of records that were marked as deleted
    return {
      deleted: result.updated, // Map the updated count to 'deleted'
    };
  }

  /**
   * Removes (soft deletes) a single document by UUID.
   * @param collectionName - Name of the collection.
   * @param id - UUID of the document.
   */
  async delete(
    collectionName: string,
    id: string
  ): Promise<{ deleted: number }> {
    try {
      // Call deleteAll to soft delete the document by UUID
      const result = await this.deleteAll(collectionName, { _uuid: id });

      // Check the result of the deleteAll operation
      if (result.deleted === 1) {
        // Successfully deleted one document
        return { deleted: 1 };
      } else if (result.deleted === 0) {
        // No document was found to delete
        throw new K2Error(
          ServiceError.NOT_FOUND,
          "Document not found",
          "sys_mdb_remove_not_found"
        );
      } else {
        // More than one document was deleted, which is unexpected
        throw new K2Error(
          ServiceError.SYSTEM_ERROR,
          "Multiple documents deleted when only one was expected",
          "sys_mdb_remove_multiple_deleted"
        );
      }
    } catch (err) {
      throw new K2Error(
        ServiceError.SYSTEM_ERROR,
        "Error removing object from collection",
        "sys_mdb_remove_upd",
        this.normalizeError(err)
      );
    }
  }

  /**
   * Permanently deletes a document that has been soft-deleted.
   * @param collectionName - Name of the collection.
   * @param id - UUID of the document.
   */
  async purge(collectionName: string, id: string): Promise<{ id: string }> {
    const collection = await this.getCollection(collectionName);

    try {
      const item = await collection.findOne({
        _uuid: id,
        _deleted: true,
      } as Filter<BaseDocument>);
      if (!item) {
        throw new K2Error(
          ServiceError.SYSTEM_ERROR,
          "Cannot purge item that is not deleted",
          "sys_mdb_gcol_pg2"
        );
      }

      await collection.deleteMany({ _uuid: id } as Filter<BaseDocument>);
      return { id };
    } catch (err) {
      if (err instanceof K2Error) {
        throw err;
      }
      throw new K2Error(
        ServiceError.SYSTEM_ERROR,
        `Error purging item with id: ${id}`,
        "sys_mdb_pg",
        this.normalizeError(err)
      );
    }
  }

  /**
   * Restores a soft-deleted document.
   * @param collectionName - Name of the collection.
   * @param criteria - Criteria to identify the document.
   */
  async restore(
    collectionName: string,
    criteria: any
  ): Promise<{ status: string; modified: number }> {
    const collection = await this.getCollection(collectionName);
    criteria._deleted = true;

    try {
      const res = await collection.updateMany(criteria, {
        $set: { _deleted: false } as Partial<BaseDocument>,
      });
      return { status: "restored", modified: res.modifiedCount };
    } catch (err) {
      throw new K2Error(
        ServiceError.SYSTEM_ERROR,
        "Error restoring a deleted item",
        "sys_mdb_pres",
        this.normalizeError(err)
      );
    }
  }

  /**
   * Counts documents based on criteria.
   * @param collectionName - Name of the collection.
   * @param criteria - Counting criteria.
   */
  async count(
    collectionName: string,
    criteria: any
  ): Promise<{ count: number }> {
    const collection = await this.getCollection(collectionName);
    try {
      const cnt = await collection.countDocuments(criteria);
      return { count: cnt };
    } catch (err) {
      throw new K2Error(
        ServiceError.SYSTEM_ERROR,
        "Error counting objects with given criteria",
        "sys_mdb_cn",
        this.normalizeError(err)
      );
    }
  }

  /**
   * Drops an entire collection.
   * @param collectionName - Name of the collection.
   */
  async drop(collectionName: string): Promise<{ status: string }> {
    const collection = await this.getCollection(collectionName);
    try {
      await collection.drop();
      return { status: "ok" };
    } catch (err) {
      throw new K2Error(
        ServiceError.SYSTEM_ERROR,
        "Error dropping collection",
        "sys_mdb_drop",
        this.normalizeError(err)
      );
    }
  }

  /**
   * Sanitizes aggregation criteria.
   * @param criteria - Aggregation stage criteria.
   */
  private static sanitiseCriteria(criteria: any): any {
    if (criteria.$match) {
      for (const key of Object.keys(criteria.$match)) {
        if (typeof criteria.$match[key] !== "string") {
          criteria.$match[key] = K2DB.sanitiseCriteria({
            [key]: criteria.$match[key],
          })[key];
        } else {
          if (key === "$exists") {
            criteria.$match[key] = criteria.$match[key] === "true";
          }
        }
      }
    }
    return criteria;
  }

  /**
   * Optional: Executes a transaction with the provided operations.
   * @param operations - A function that performs operations within a transaction session.
   */
  async executeTransaction(
    operations: (session: any) => Promise<void>
  ): Promise<void> {
    const session = this.connection.startSession();
    session.startTransaction();
    try {
      await operations(session);
      await session.commitTransaction();
    } catch (error) {
      await session.abortTransaction();
      throw this.normalizeError(error);
    } finally {
      session.endSession();
    }
  }

  /**
   * Optional: Creates an index on the specified collection.
   * @param collectionName - Name of the collection.
   * @param indexSpec - Specification of the index.
   * @param options - Optional index options.
   */
  async createIndex(
    collectionName: string,
    indexSpec: any,
    options?: any
  ): Promise<void> {
    const collection = await this.getCollection(collectionName);
    try {
      await collection.createIndex(indexSpec, options);
      debug(`Index created on ${collectionName}: ${JSON.stringify(indexSpec)}`);
    } catch (err) {
      throw new K2Error(
        ServiceError.SYSTEM_ERROR,
        `Error creating index on ${collectionName}`,
        "sys_mdb_idx",
        this.normalizeError(err)
      );
    }
  }

  /**
   * Releases the MongoDB connection.
   */
  async release(): Promise<void> {
    await this.connection.close();
    debug("MongoDB connection released");
  }

  /**
   * Closes the MongoDB connection.
   */
  close(): void {
    this.connection.close();
  }

  /**
   * Drops the entire database.
   */
  public async dropDatabase(): Promise<void> {
    try {
      await this.db.dropDatabase();
      debug("Database dropped successfully");
    } catch (err) {
      throw new K2Error(
        ServiceError.SYSTEM_ERROR,
        "Error dropping database",
        "sys_mdb_drop_db",
        this.normalizeError(err)
      );
    }
  }

  /**
   * Validates the MongoDB collection name.
   * @param collectionName - The name of the collection to validate.
   * @throws {K2Error} - If the collection name is invalid.
   */
  public validateCollectionName(collectionName: string): void {
    // Check for null character
    if (collectionName.includes("\0")) {
      throw new K2Error(
        ServiceError.BAD_REQUEST,
        "Collection name cannot contain null characters",
        "sys_mdb_invalid_collection_name"
      );
    }

    // Check if it starts with 'system.'
    if (collectionName.startsWith("system.")) {
      throw new K2Error(
        ServiceError.BAD_REQUEST,
        "Collection name cannot start with 'system.'",
        "sys_mdb_invalid_collection_name"
      );
    }

    // Check for invalid characters (e.g., '$')
    if (collectionName.includes("$")) {
      throw new K2Error(
        ServiceError.BAD_REQUEST,
        "Collection name cannot contain the '$' character",
        "sys_mdb_invalid_collection_name"
      );
    }

    // Additional checks can be added here as needed
  }

  /**
   * Optional: Checks the health of the database connection.
   */
  async isHealthy(): Promise<boolean> {
    try {
      await this.db.command({ ping: 1 });
      return true;
    } catch {
      return false;
    }
  }

  /**
   * Utility to normalize the error type.
   * @param err - The caught error of type `unknown`.
   * @returns A normalized error of type `Error`.
   */
  private normalizeError(err: unknown): Error {
    return err instanceof Error ? err : new Error(String(err));
  }
}
