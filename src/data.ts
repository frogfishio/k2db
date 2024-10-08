import { K2DB, BaseDocument } from "./db";

export class K2Data {
  constructor(private db: K2DB, private owner: string) {}

  /**
   * Retrieves a single document by UUID.
   * @param collectionName - Name of the collection.
   * @param uuid - UUID of the document.
   */
  async get(collectionName: string, uuid: string): Promise<BaseDocument> {
    return this.db.get(collectionName, uuid);
  }

  /**
   * Retrieves a single document matching the criteria.
   * @param collectionName - Name of the collection.
   * @param criteria - Search criteria.
   * @param fields - Optional fields to include.
   */
  async findOne(
    collectionName: string,
    criteria: any,
    fields?: Array<string>
  ): Promise<BaseDocument | null> {
    return this.db.findOne(collectionName, criteria, fields);
  }

  /**
   * Finds documents based on filter with optional parameters and pagination.
   */
  async find(
    collectionName: string,
    filter: any,
    params?: any,
    skip?: number,
    limit?: number
  ): Promise<BaseDocument[]> {
    return this.db.find(collectionName, filter, params, skip, limit);
  }

  /**
   * Aggregates documents based on criteria with pagination support.
   */
  async aggregate(
    collectionName: string,
    criteria: any[],
    skip?: number,
    limit?: number
  ): Promise<BaseDocument[]> {
    return this.db.aggregate(collectionName, criteria, skip, limit);
  }

  /**
   * Creates a new document in the collection.
   */
  async create(
    collectionName: string,
    data: Partial<BaseDocument>
  ): Promise<{ id: string }> {
    return this.db.create(collectionName, this.owner, data);
  }

  /**
   * Updates multiple documents based on criteria.
   */
  async updateAll(
    collectionName: string,
    criteria: any,
    values: Partial<BaseDocument>
  ): Promise<{ updated: number }> {
    // Ensure it returns { updated: number }
    return this.db.updateAll(collectionName, criteria, values);
  }

  /**
   * Updates a single document by UUID.
   */
  async update(
    collectionName: string,
    id: string,
    data: Partial<BaseDocument>,
    replace: boolean = false
  ): Promise<{ updated: number }> {
    // Ensure it returns { updated: number }
    return this.db.update(collectionName, id, data, replace);
  }

  /**
   * Removes (soft deletes) multiple documents based on criteria.
   */
  async deleteAll(
    collectionName: string,
    criteria: any
  ): Promise<{ deleted: number }> {
    // Ensure it returns { deleted: number }
    return this.db.deleteAll(collectionName, criteria);
  }

  /**
   * Removes (soft deletes) a single document by UUID.
   */
  async delete(
    collectionName: string,
    id: string
  ): Promise<{ deleted: number }> {
    return this.db.delete(collectionName, id);
  }

  /**
   * Permanently deletes a document that has been soft-deleted.
   */
  async purge(collectionName: string, id: string): Promise<{ id: string }> {
    return this.db.purge(collectionName, id);
  }

  /**
   * Restores a soft-deleted document.
   */
  async restore(
    collectionName: string,
    criteria: any
  ): Promise<{ status: string; modified: number }> {
    return this.db.restore(collectionName, criteria);
  }

  /**
   * Counts documents based on criteria.
   */
  async count(
    collectionName: string,
    criteria: any
  ): Promise<{ count: number }> {
    return this.db.count(collectionName, criteria);
  }

  /**
   * Drops an entire collection.
   */
  async drop(collectionName: string): Promise<{ status: string }> {
    return this.db.drop(collectionName);
  }

  /**
   * Executes a transaction with the provided operations.
   */
  async executeTransaction(
    operations: (session: any) => Promise<void>
  ): Promise<void> {
    return this.db.executeTransaction(operations);
  }

  /**
   * Creates an index on the specified collection.
   */
  async createIndex(
    collectionName: string,
    indexSpec: any,
    options?: any
  ): Promise<void> {
    return this.db.createIndex(collectionName, indexSpec, options);
  }

  /**
   * Drops the entire database.
   */
  async dropDatabase(): Promise<void> {
    return this.db.dropDatabase();
  }

  /**
   * Checks the health of the database connection.
   */
  async isHealthy(): Promise<boolean> {
    return this.db.isHealthy();
  }
}
