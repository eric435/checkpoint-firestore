import { Firestore, DocumentData, Query } from "firebase-admin/firestore";
import type { RunnableConfig } from "@langchain/core/runnables";
import {
  BaseCheckpointSaver,
  type Checkpoint,
  type CheckpointListOptions,
  type CheckpointTuple,
  type SerializerProtocol,
  type PendingWrite,
  type CheckpointMetadata,
  CheckpointPendingWrite,
} from "@langchain/langgraph-checkpoint";



export type FirestoreSaverParams = {
  db: Firestore;
  checkpointCollectionName?: string;
};

/**
 * A LangGraph checkpoint saver backed by a Firestore database.
 */
export class FirestoreSaver extends BaseCheckpointSaver {
  protected db: Firestore;

  checkpointCollectionName = "checkpoints";

  constructor(
    { db, checkpointCollectionName }: FirestoreSaverParams,
    serde?: SerializerProtocol
  ) {
    super(serde);
    this.db = db;
    this.checkpointCollectionName =
      checkpointCollectionName ?? this.checkpointCollectionName;
  }

  /**
   * Retrieves a checkpoint from the Firestore database based on the
   * provided config. If the config contains a "checkpoint_id" key, the checkpoint with
   * the matching thread ID and checkpoint ID is retrieved. Otherwise, the latest checkpoint
   * for the given thread ID is retrieved.
   */
  async getTuple(config: RunnableConfig): Promise<CheckpointTuple | undefined> {
    const {
      thread_id,
      checkpoint_ns = "default_ns",
      checkpoint_id,
    } = config.configurable ?? {};

    if (!thread_id) {
      return undefined;
    }

    let doc: DocumentData & { checkpoint_id: string };

    if (checkpoint_id) {
      // Get specific checkpoint document by filtering on checkpoint_id field
      const querySnapshot = await this.db
        .collection(this.checkpointCollectionName)
        .doc(thread_id)
        .collection(checkpoint_ns)
        .where("checkpoint_id", "==", checkpoint_id)
        .where("doc_type", "==", "checkpoint")
        .limit(1)
        .get();

      if (querySnapshot.empty) {
        return undefined;
      }
      doc = querySnapshot.docs[0].data() as DocumentData & {
        checkpoint_id: string;
      };
    } else {
      // Get latest checkpoint by sorting checkpoint_id desc and taking first
      const querySnapshot = await this.db
        .collection(this.checkpointCollectionName)
        .doc(thread_id)
        .collection(checkpoint_ns)
        .where("doc_type", "==", "checkpoint")
        .orderBy("checkpoint_id", "desc")
        .limit(1)
        .get();

      if (querySnapshot.empty) {
        return undefined;
      }

      const docSnapshot = querySnapshot.docs[0];
      doc = docSnapshot.data() as DocumentData & { checkpoint_id: string };
    }

    const configurableValues = {
      thread_id,
      checkpoint_ns: checkpoint_ns === "default_ns" ? "" : checkpoint_ns,
      checkpoint_id: doc.checkpoint_id,
    };

    const checkpoint = (await this.serde.loadsTyped(
      doc.type,
      doc.checkpoint
    )) as Checkpoint;

    // Get pending writes from same collection using checkpoint_id filter
    const writesSnapshot = await this.db
      .collection(this.checkpointCollectionName)
      .doc(thread_id)
      .collection(checkpoint_ns)
      .where("checkpoint_id", "==", doc.checkpoint_id)
      .where("doc_type", "==", "write")
      .get();

    const pendingWrites: CheckpointPendingWrite[] = await Promise.all(
      writesSnapshot.docs.map(async (writeDoc) => {
        const writeData = writeDoc.data();
        return [
          writeData.task_id,
          writeData.channel,
          await this.serde.loadsTyped(writeData.type, writeData.value),
        ] as CheckpointPendingWrite;
      })
    );

    return {
      config: { configurable: configurableValues },
      checkpoint,
      pendingWrites,
      metadata: (await this.serde.loadsTyped(
        doc.metadata_type,
        doc.metadata
      )) as CheckpointMetadata,
      parentConfig:
        doc.parent_checkpoint_id != null
          ? {
              configurable: {
                thread_id,
                checkpoint_ns:
                  checkpoint_ns === "default_ns" ? "" : checkpoint_ns,
                checkpoint_id: doc.parent_checkpoint_id,
              },
            }
          : undefined,
    };
  }

  /**
   * Retrieve a list of checkpoint tuples from the MongoDB database based
   * on the provided config. The checkpoints are ordered by checkpoint ID
   * in descending order (newest first).
   */
  async *list(
    config: RunnableConfig,
    options?: CheckpointListOptions
  ): AsyncGenerator<CheckpointTuple> {
    // Extract pagination and filtering options from the options parameter
    const { limit, before, filter } = options ?? {};

    // Get thread_id and checkpoint_ns from config
    const thread_id = config?.configurable?.thread_id;
    const checkpoint_ns = config?.configurable?.checkpoint_ns ?? "default_ns";

    // If no thread_id is provided, we can't query anything
    if (!thread_id) {
      return;
    }

    // Build Firestore query filtering for checkpoint documents only
    let query = this.db
      .collection(this.checkpointCollectionName)
      .doc(thread_id)
      .collection(checkpoint_ns)
      .where("doc_type", "==", "checkpoint")
      .orderBy("checkpoint_id", "desc");

    // Handle pagination with 'before' cursor using checkpoint_id comparison
    if (before?.configurable?.checkpoint_id) {
      query = query.where(
        "checkpoint_id",
        "<",
        before.configurable.checkpoint_id
      );
    }

    // Apply metadata filters if provided
    // The 'filter' option allows filtering by arbitrary metadata fields
    // e.g., filter: { "user_id": "123", "session_type": "chat" }
    // This gets translated to query conditions on metadata.user_id, metadata.session_type, etc.
    if (filter) {
      Object.entries(filter).forEach(([key, value]) => {
        query = query.where(`metadata.${key}`, "==", value);
      });
    }

    // Apply limit if specified
    // The 'limit' option controls how many results to return in this batch
    // Used for pagination and performance - prevents loading too many checkpoints at once
    if (limit !== undefined) {
      query = query.limit(limit);
    }

    // Execute the Firestore query
    const querySnapshot = await query.get();

    // Iterate through results and yield CheckpointTuple objects
    for (const docSnapshot of querySnapshot.docs) {
      const doc = docSnapshot.data();
      const { checkpoint_id } = doc;

      // Deserialize the checkpoint data from stored format
      const checkpoint = (await this.serde.loadsTyped(
        doc.type,
        doc.checkpoint
      )) as Checkpoint;

      // Deserialize the metadata from stored format
      const metadata = (await this.serde.loadsTyped(
        doc.metadata_type,
        doc.metadata
      )) as CheckpointMetadata;

      // Yield a complete CheckpointTuple with config, checkpoint data, and parent reference
      yield {
        config: {
          configurable: {
            thread_id,
            checkpoint_ns: checkpoint_ns === "default_ns" ? "" : checkpoint_ns,
            checkpoint_id,
          },
        },
        checkpoint,
        metadata,
        // Include parent checkpoint reference if this checkpoint has a parent
        // This allows traversing the checkpoint chain backwards
        parentConfig: doc.parent_checkpoint_id
          ? {
              configurable: {
                thread_id,
                checkpoint_ns:
                  checkpoint_ns === "default_ns" ? "" : checkpoint_ns,
                checkpoint_id: doc.parent_checkpoint_id,
              },
            }
          : undefined,
      };
    }
  }

  /**
   * Saves a checkpoint to the Firestore database. The checkpoint is associated
   * with the provided config and its parent config (if any).
   */
  async put(
    config: RunnableConfig,
    checkpoint: Checkpoint,
    metadata: CheckpointMetadata
  ): Promise<RunnableConfig> {
    const thread_id = config.configurable?.thread_id;
    const checkpoint_ns = !config.configurable?.checkpoint_ns || config.configurable?.checkpoint_ns === "" ?
        "default_ns" :
        config.configurable?.checkpoint_ns;
    const checkpoint_id = checkpoint.id;
    if (thread_id === undefined) {
      throw new Error(
        `The provided config must contain a configurable field with a "thread_id" field.`
      );
    }
    const [
      [checkpointType, serializedCheckpoint],
      [metadataType, serializedMetadata],
    ] = await Promise.all([
      this.serde.dumpsTyped(checkpoint),
      this.serde.dumpsTyped(metadata),
    ]);

    if (checkpointType !== metadataType) {
      throw new Error("Mismatched checkpoint and metadata types.");
    }

    const doc = {
      doc_type: "checkpoint",
      checkpoint_id,
      parent_checkpoint_id: config.configurable?.checkpoint_id ?? null,
      type: checkpointType,
      checkpoint: serializedCheckpoint,
      metadata_type: metadataType,
      metadata: serializedMetadata,
    };

    // Implement upsert behavior: check if document with this checkpoint_id exists
    const existingQuery = await this.db
      .collection(this.checkpointCollectionName)
      .doc(thread_id)
      .collection(checkpoint_ns)
      .where("checkpoint_id", "==", checkpoint_id)
      .where("doc_type", "==", "checkpoint")
      .limit(1)
      .get();

    if (!existingQuery.empty) {
      // Update existing document
      const existingDocRef = existingQuery.docs[0].ref;
      await existingDocRef.set(doc, { merge: true });
    } else {
      // Create new document with auto-generated ID
      await this.db
        .collection(this.checkpointCollectionName)
        .doc(thread_id)
        .collection(checkpoint_ns)
        .add(doc);
    }

    return {
      configurable: {
        thread_id,
        checkpoint_ns: checkpoint_ns === "default_ns" ? "" : checkpoint_ns,
        checkpoint_id,
      },
    };
  }


  /**
   * Saves intermediate writes associated with a checkpoint to the Firestore database.
   */
  async putWrites(
    config: RunnableConfig,
    writes: PendingWrite[],
    taskId: string
  ): Promise<void> {
    const thread_id = config.configurable?.thread_id;
    const checkpoint_ns =
      !config.configurable?.checkpoint_ns ||
      config.configurable?.checkpoint_ns === ""
        ? "default_ns"
        : config.configurable?.checkpoint_ns;
    const checkpoint_id = config.configurable?.checkpoint_id;
    if (
      thread_id === undefined ||
      checkpoint_ns === undefined ||
      checkpoint_id === undefined
    ) {
      throw new Error(
        `The provided config must contain a configurable field with "thread_id", "checkpoint_ns" and "checkpoint_id" fields.`
      );
    }

    // Process each write with upsert behavior
    await Promise.all(
      writes.map(async ([channel, value], idx) => {
        const [type, serializedValue] = await this.serde.dumpsTyped(value);

        const writeData = {
          doc_type: "write",
          checkpoint_id,
          task_id: taskId,
          channel,
          type,
          value: serializedValue,
          idx,
        };

        // Check if document with this compound key exists
        const existingQuery = await this.db
          .collection(this.checkpointCollectionName)
          .doc(thread_id)
          .collection(checkpoint_ns)
          .where("doc_type", "==", "write")
          .where("checkpoint_id", "==", checkpoint_id)
          .where("task_id", "==", taskId)
          .where("channel", "==", channel)
          .where("idx", "==", idx)
          .limit(1)
          .get();

        if (!existingQuery.empty) {
          // Update existing document
          const existingDocRef = existingQuery.docs[0].ref;
          await existingDocRef.set(writeData, { merge: true });
        } else {
          // Create new document with auto-generated ID
          await this.db
            .collection(this.checkpointCollectionName)
            .doc(thread_id)
            .collection(checkpoint_ns)
            .add(writeData);
        }
      })
    );
  }

  async deleteThread(threadId: string) {
    // Get the thread document reference
    const checkpointDocRef = this.db
      .collection(this.checkpointCollectionName)
      .doc(threadId);

    // Delete checkpoint subcollections (checkpoint namespaces)
    const checkpointSubcollections = await checkpointDocRef.listCollections();
    for (const subcollection of checkpointSubcollections) {
      await this.deleteCollection(subcollection.path);
    }

    // Finally delete the thread document itself
    await checkpointDocRef.delete();
  }

  /**
   * Delete a collection using batched deletes
   * Based on Firestore documentation recommendations
   */
  private async deleteCollection(
    collectionPath: string,
    batchSize: number = 100
  ): Promise<void> {
    const collectionRef = this.db.collection(collectionPath);
    const query = collectionRef.limit(batchSize);

    return new Promise((resolve, reject) => {
      this.deleteQueryBatch(query, resolve, reject).catch(reject);
    });
  }

  private async deleteQueryBatch(
    query: Query<DocumentData>,
    resolve: () => void,
    reject: (error: Error) => void
  ): Promise<void> {
    try {
      const snapshot = await query.get();

      const batchSize = snapshot.size;
      if (batchSize === 0) {
        // When there are no documents left, we are done
        resolve();
        return;
      }

      // Delete documents in a batch
      const batch = this.db.batch();

      for (const doc of snapshot.docs) {
        batch.delete(doc.ref);
      }

      await batch.commit();

      // Recurse on the next process tick, to avoid exploding the stack
      process.nextTick(() => {
        this.deleteQueryBatch(query, resolve, reject).catch(reject);
      });
    } catch (error) {
      reject(error as Error);
    }
  }
}