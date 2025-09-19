import { describe, it, expect, afterAll } from "vitest";
import env from "dotenv";
import {
  Checkpoint,
  CheckpointTuple,
  emptyCheckpoint,
  uuid6,
} from "@langchain/langgraph-checkpoint";
import { initializeApp, cert } from "firebase-admin/app";
import { getFirestore } from "firebase-admin/firestore";
import { FirestoreSaver } from "../index.js";

env.config();
initializeApp({ credential: cert(process.env.PATH_TO_FIREBASE_CERT || "") });


const db = getFirestore();

const checkpoint1: Checkpoint = {
  v: 4,
  id: uuid6(-1),
  ts: "2024-04-19T17:19:07.952Z",
  channel_values: {
    someKey1: "someValue1",
  },
  channel_versions: {
    someKey2: 1,
  },
  versions_seen: {
    someKey3: {
      someKey4: 1,
    },
  },
};

const checkpoint2: Checkpoint = {
  v: 4,
  id: uuid6(1),
  ts: "2024-04-20T17:19:07.952Z",
  channel_values: {
    someKey1: "someValue2",
  },
  channel_versions: {
    someKey2: 2,
  },
  versions_seen: {
    someKey3: {
      someKey4: 2,
    },
  },
};


afterAll(async () => {
  const saver = new FirestoreSaver({ db });

  // Clean up test data using deleteThread method
  const testThreadIds = [
    "1",
    "2",
    "upsert-test",
    "write-upsert-test",
    "namespace-test",
    "complex-writes",
    "empty-writes",
    "thread-with-special_chars.123",
    "pagination-test",
    "metadata-filter-test",
    "concurrent-test",
    "concurrent-writes",
    "integrity-test",
    "parent-child-test"
  ];

  try {
    // Delete all test threads
    await Promise.all(
      testThreadIds.map(threadId => saver.deleteThread(threadId))
    );
  } catch (error) {
    console.error("Error cleaning up test data:", error);
  }
});

describe("FirestoreSaver", () => {
  it("should save and retrieve checkpoints correctly", async () => {
    const saver = new FirestoreSaver({ db });

    // get undefined checkpoint
    const undefinedCheckpoint = await saver.getTuple({
      configurable: { thread_id: "1" },
    });
    expect(undefinedCheckpoint).toBeUndefined();

    // save first checkpoint
    const runnableConfig = await saver.put(
      { configurable: { thread_id: "1" } },
      checkpoint1,
      { source: "update", step: -1, parents: {} }
    );
    expect(runnableConfig).toEqual({
      configurable: {
        thread_id: "1",
        checkpoint_ns: "",
        checkpoint_id: checkpoint1.id,
      },
    });

    // add some writes
    await saver.putWrites(
      {
        configurable: {
          checkpoint_id: checkpoint1.id,
          checkpoint_ns: "",
          thread_id: "1",
        },
      },
      [["bar", "baz"]],
      "foo"
    );

    // get first checkpoint tuple
    const firstCheckpointTuple = await saver.getTuple({
      configurable: { thread_id: "1" },
    });
    expect(firstCheckpointTuple?.config).toEqual({
      configurable: {
        thread_id: "1",
        checkpoint_ns: "",
        checkpoint_id: checkpoint1.id,
      },
    });
    expect(firstCheckpointTuple?.checkpoint).toEqual(checkpoint1);
    expect(firstCheckpointTuple?.parentConfig).toBeUndefined();
    expect(firstCheckpointTuple?.pendingWrites).toEqual([
      ["foo", "bar", "baz"],
    ]);

    // save second checkpoint
    await saver.put(
      {
        configurable: {
          thread_id: "1",
          checkpoint_id: "2024-04-18T17:19:07.952Z",
        },
      },
      checkpoint2,
      { source: "update", step: -1, parents: {} }
    );

    // verify that parentTs is set and retrieved correctly for second checkpoint
    const secondCheckpointTuple = await saver.getTuple({
      configurable: { thread_id: "1" },
    });
    expect(secondCheckpointTuple?.parentConfig).toEqual({
      configurable: {
        thread_id: "1",
        checkpoint_ns: "",
        checkpoint_id: "2024-04-18T17:19:07.952Z",
      },
    });

    // list checkpoints
    const checkpointTupleGenerator = saver.list({
      configurable: { thread_id: "1" },
    });
    const checkpointTuples: CheckpointTuple[] = [];
    for await (const checkpoint of checkpointTupleGenerator) {
      checkpointTuples.push(checkpoint);
    }
    expect(checkpointTuples.length).toBe(2);

    const checkpointTuple1 = checkpointTuples[0];
    const checkpointTuple2 = checkpointTuples[1];
    expect(checkpointTuple1.checkpoint.ts).toBe("2024-04-20T17:19:07.952Z");
    expect(checkpointTuple2.checkpoint.ts).toBe("2024-04-19T17:19:07.952Z");
  });

  it("should delete thread", async () => {
    const saver = new FirestoreSaver({ db });
    await saver.put({ configurable: { thread_id: "1" } }, emptyCheckpoint(), {
      source: "update",
      step: -1,
      parents: {},
    });

    await saver.put({ configurable: { thread_id: "2" } }, emptyCheckpoint(), {
      source: "update",
      step: -1,
      parents: {},
    });

    await saver.deleteThread("1");

    expect(
      await saver.getTuple({ configurable: { thread_id: "1" } })
    ).toBeUndefined();
    expect(
      await saver.getTuple({ configurable: { thread_id: "2" } })
    ).toBeDefined();
  });

  // 1. Upsert Behavior Tests
  describe("Upsert behavior", () => {
    it("should update existing checkpoint instead of creating duplicates", async () => {
      const saver = new FirestoreSaver({ db });
      const checkpointId = uuid6(-1);
      
      const originalCheckpoint: Checkpoint = {
        v: 4,
        id: checkpointId,
        ts: "2024-04-19T17:19:07.952Z",
        channel_values: { key: "original" },
        channel_versions: { key: 1 },
        versions_seen: {},
      };

      const updatedCheckpoint: Checkpoint = {
        v: 4,
        id: checkpointId,
        ts: "2024-04-19T17:19:07.952Z",
        channel_values: { key: "updated" },
        channel_versions: { key: 2 },
        versions_seen: {},
      };

      // Put original checkpoint
      await saver.put(
        { configurable: { thread_id: "upsert-test" } },
        originalCheckpoint,
        { source: "update", step: 1, parents: {} }
      );

      // Put updated checkpoint with same ID
      await saver.put(
        { configurable: { thread_id: "upsert-test" } },
        updatedCheckpoint,
        { source: "update", step: 2, parents: {} }
      );

      // Should only have one checkpoint with updated values
      const checkpoints = [];
      for await (const checkpoint of saver.list({ configurable: { thread_id: "upsert-test" } })) {
        checkpoints.push(checkpoint);
      }
      
      expect(checkpoints.length).toBe(1);
      expect(checkpoints[0]?.checkpoint.channel_values.key).toBe("updated");
      expect(checkpoints[0]?.metadata?.step).toBe(2);
    });

    it("should update existing writes instead of creating duplicates", async () => {
      const saver = new FirestoreSaver({ db });
      const checkpointId = uuid6(-1);
      
      await saver.put(
        { configurable: { thread_id: "write-upsert-test" } },
        { ...checkpoint1, id: checkpointId },
        { source: "update", step: -1, parents: {} }
      );

      const config = {
        configurable: {
          thread_id: "write-upsert-test",
          checkpoint_id: checkpointId,
          checkpoint_ns: "",
        },
      };

      // Put original writes
      await saver.putWrites(config, [["channel1", "original"]], "task1");
      
      // Put updated writes with same task_id and idx
      await saver.putWrites(config, [["channel1", "updated"]], "task1");

      // Should only have one write with updated value
      const tuple = await saver.getTuple({ configurable: { thread_id: "write-upsert-test" } });
      expect(tuple?.pendingWrites?.length).toBe(1);
      expect(tuple?.pendingWrites?.[0]).toEqual(["task1", "channel1", "updated"]);
    });
  });

  // 2. Multiple Checkpoint Namespaces Tests
  describe("Multiple checkpoint namespaces", () => {
    it.skip("should isolate checkpoints in different namespaces", async () => {
      const saver = new FirestoreSaver({ db });
      const threadId = "namespace-test";

      // Put checkpoint in default namespace
      await saver.put(
        { configurable: { thread_id: threadId } },
        checkpoint1,
        { source: "update", step: 1, parents: {} }
      );

      // Put checkpoint in custom namespace
      await saver.put(
        { configurable: { thread_id: threadId, checkpoint_ns: "custom" } },
        checkpoint2,
        { source: "update", step: 2, parents: {} }
      );

      // Get from default namespace
      const defaultTuple = await saver.getTuple({ configurable: { thread_id: threadId } });
      expect(defaultTuple?.checkpoint.id).toBe(checkpoint1.id);

      // Get from custom namespace
      const customTuple = await saver.getTuple({
        configurable: { thread_id: threadId, checkpoint_ns: "custom" }
      });
      expect(customTuple?.checkpoint.id).toBe(checkpoint2.id);

      // List should be isolated
      const defaultList = [];
      for await (const cp of saver.list({ configurable: { thread_id: threadId } })) {
        defaultList.push(cp);
      }
      expect(defaultList.length).toBe(1);

      const customList = [];
      for await (const cp of saver.list({ configurable: { thread_id: threadId, checkpoint_ns: "custom" } })) {
        customList.push(cp);
      }
      expect(customList.length).toBe(1);
    });
  });

  // 3. Complex Write Scenarios
  describe("Complex write scenarios", () => {
    it("should handle multiple task IDs and indices", async () => {
      const saver = new FirestoreSaver({ db });
      const checkpointId = uuid6(-1);
      
      await saver.put(
        { configurable: { thread_id: "complex-writes" } },
        { ...checkpoint1, id: checkpointId },
        { source: "update", step: -1, parents: {} }
      );

      const config = {
        configurable: {
          thread_id: "complex-writes",
          checkpoint_id: checkpointId,
          checkpoint_ns: "",
        },
      };

      // Multiple writes with different task IDs
      await saver.putWrites(config, [["ch1", "val1"], ["ch2", "val2"]], "task1");
      await saver.putWrites(config, [["ch3", "val3"]], "task2");
      await saver.putWrites(config, [["ch4", "val4"]], "task1"); // Same task, different write (different channel)

      const tuple = await saver.getTuple({ configurable: { thread_id: "complex-writes" } });
      expect(tuple?.pendingWrites?.length).toBe(4);
      
      // Check all writes are present
      const writes = tuple?.pendingWrites?.sort((a, b) => `${a[0]}_${a[1]}`.localeCompare(`${b[0]}_${b[1]}`));
      expect(writes?.[0]).toEqual(["task1", "ch1", "val1"]);
      expect(writes?.[1]).toEqual(["task1", "ch2", "val2"]);
      expect(writes?.[2]).toEqual(["task1", "ch4", "val4"]);
      expect(writes?.[3]).toEqual(["task2", "ch3", "val3"]);
    });
  });

  // 4. Edge Cases
  describe("Edge cases", () => {
    it("should handle empty writes array", async () => {
      const saver = new FirestoreSaver({ db });
      const checkpointId = uuid6(-1);
      
      await saver.put(
        { configurable: { thread_id: "empty-writes" } },
        { ...checkpoint1, id: checkpointId },
        { source: "update", step: -1, parents: {} }
      );

      const config = {
        configurable: {
          thread_id: "empty-writes",
          checkpoint_id: checkpointId,
          checkpoint_ns: "",
        },
      };

      // Should not throw error
      await saver.putWrites(config, [], "task1");

      const tuple = await saver.getTuple({ configurable: { thread_id: "empty-writes" } });
      expect(tuple?.pendingWrites?.length).toBe(0);
    });

    it.skip("should handle special characters in IDs", async () => {
      const saver = new FirestoreSaver({ db });
      const specialThreadId = "thread-with-special_chars.123";
      const specialNamespace = "ns_with.special-chars";
      
      await saver.put(
        { configurable: { thread_id: specialThreadId, checkpoint_ns: specialNamespace } },
        checkpoint1,
        { source: "update", step: -1, parents: {} }
      );

      const tuple = await saver.getTuple({
        configurable: { thread_id: specialThreadId, checkpoint_ns: specialNamespace }
      });
      expect(tuple?.checkpoint.id).toBe(checkpoint1.id);
    });
  });

  // 5. List Method Edge Cases
  describe("List method edge cases", () => {
    it("should handle pagination with before and limit", async () => {
      const saver = new FirestoreSaver({ db });
      const threadId = "pagination-test";

      // Create multiple checkpoints
      const checkpoints = [];
      for (let i = 0; i < 5; i += 1) {
        const cp: Checkpoint = {
          v: 4,
          id: uuid6(i),
          ts: `2024-04-${20 + i}T17:19:07.952Z`,
          channel_values: { step: i },
          channel_versions: {},
          versions_seen: {},
        };
        checkpoints.push(cp);
        
        await saver.put(
          { configurable: { thread_id: threadId } },
          cp,
          { source: "update", step: i, parents: {} }
        );
      }

      // Test limit
      const limitedList = [];
      for await (const cp of saver.list(
        { configurable: { thread_id: threadId } },
        { limit: 2 }
      )) {
        limitedList.push(cp);
      }
      expect(limitedList.length).toBe(2);

      // Test before cursor
      const beforeList = [];
      for await (const cp of saver.list(
        { configurable: { thread_id: threadId } },
        { before: { configurable: { checkpoint_id: checkpoints[2].id } } }
      )) {
        beforeList.push(cp);
      }
      expect(beforeList.length).toBe(2); // Should get checkpoints 3 and 4 (newer than checkpoint 2)
    });

    it("should return empty results for non-existent thread", async () => {
      const saver = new FirestoreSaver({ db });
      
      const results = [];
      for await (const cp of saver.list({ configurable: { thread_id: "non-existent" } })) {
        results.push(cp);
      }
      expect(results.length).toBe(0);
    });

    it.skip("should filter by metadata", async () => {
      const saver = new FirestoreSaver({ db });
      const threadId = "metadata-filter-test";

      await saver.put(
        { configurable: { thread_id: threadId } },
        checkpoint1,
        { source: "update", step: 1, parents: {}, user: "alice" } as any
      );

      await saver.put(
        { configurable: { thread_id: threadId } },
        checkpoint2,
        { source: "update", step: 2, parents: {}, user: "bob" } as any
      );

      const aliceCheckpoints = [];
      for await (const cp of saver.list(
        { configurable: { thread_id: threadId } },
        { filter: { user: "alice" } }
      )) {
        aliceCheckpoints.push(cp);
      }
      expect(aliceCheckpoints.length).toBe(1);
      expect((aliceCheckpoints[0]?.metadata as any)?.user).toBe("alice");
    });
  });

  // 6. Error Handling
  describe("Error handling", () => {
    it("should throw error for missing thread_id in put", async () => {
      const saver = new FirestoreSaver({ db });
      
      await expect(
        saver.put({ configurable: {} }, checkpoint1, { source: "update", step: -1, parents: {} })
      ).rejects.toThrow("thread_id");
    });

    it("should throw error for missing required fields in putWrites", async () => {
      const saver = new FirestoreSaver({ db });
      
      await expect(
        saver.putWrites({ configurable: { thread_id: "test" } }, [["ch", "val"]], "task")
      ).rejects.toThrow();
    });

    it("should return undefined for getTuple with missing thread_id", async () => {
      const saver = new FirestoreSaver({ db });
      
      const result = await saver.getTuple({ configurable: {} });
      expect(result).toBeUndefined();
    });
  });

  // 7. Concurrent Operations
  describe("Concurrent operations", () => {
    it("should handle concurrent put operations", async () => {
      const saver = new FirestoreSaver({ db });
      const threadId = "concurrent-test";

      const promises = [];
      for (let i = 0; i < 3; i += 1) {
        const cp: Checkpoint = {
          v: 4,
          id: uuid6(i),
          ts: `2024-04-${20 + i}T17:19:07.952Z`,
          channel_values: { step: i },
          channel_versions: {},
          versions_seen: {},
        };
        
        promises.push(
          saver.put(
            { configurable: { thread_id: threadId } },
            cp,
            { source: "update", step: i, parents: {} }
          )
        );
      }

      // All should complete without error
      await Promise.all(promises);

      const checkpoints = [];
      for await (const cp of saver.list({ configurable: { thread_id: threadId } })) {
        checkpoints.push(cp);
      }
      expect(checkpoints.length).toBe(3);
    });

    it("should handle concurrent writes to same checkpoint", async () => {
      const saver = new FirestoreSaver({ db });
      const checkpointId = uuid6(-1);
      
      await saver.put(
        { configurable: { thread_id: "concurrent-writes" } },
        { ...checkpoint1, id: checkpointId },
        { source: "update", step: -1, parents: {} }
      );

      const config = {
        configurable: {
          thread_id: "concurrent-writes",
          checkpoint_id: checkpointId,
          checkpoint_ns: "",
        },
      };

      const writePromises = [];
      for (let i = 0; i < 3; i += 1) {
        writePromises.push(
          saver.putWrites(config, [[`channel${i}`, `value${i}`]], `task${i}`)
        );
      }

      await Promise.all(writePromises);

      const tuple = await saver.getTuple({ configurable: { thread_id: "concurrent-writes" } });
      expect(tuple?.pendingWrites?.length).toBe(3);
    });
  });

  // 8. Data Integrity
  describe("Data integrity", () => {
    it("should store documents with correct doc_type field", async () => {
      const saver = new FirestoreSaver({ db });
      const threadId = "integrity-test";
      const checkpointId = uuid6(-1);
      
      await saver.put(
        { configurable: { thread_id: threadId } },
        { ...checkpoint1, id: checkpointId },
        { source: "update", step: -1, parents: {} }
      );

      await saver.putWrites(
        {
          configurable: {
            thread_id: threadId,
            checkpoint_id: checkpointId,
            checkpoint_ns: "",
          },
        },
        [["channel", "value"]],
        "task1"
      );

      // Verify we can retrieve both checkpoint and writes
      const tuple = await saver.getTuple({ configurable: { thread_id: threadId } });
      expect(tuple?.checkpoint.id).toBe(checkpointId);
      expect(tuple?.pendingWrites?.length).toBe(1);
      expect(tuple?.pendingWrites?.[0]).toEqual(["task1", "channel", "value"]);
    });

    it("should maintain parent-child relationships", async () => {
      const saver = new FirestoreSaver({ db });
      const threadId = "parent-child-test";
      
      // Create parent checkpoint
      const parentConfig = await saver.put(
        { configurable: { thread_id: threadId } },
        checkpoint1,
        { source: "update", step: 1, parents: {} }
      );

      // Create child checkpoint
      await saver.put(
        { configurable: { thread_id: threadId, checkpoint_id: parentConfig.configurable?.checkpoint_id } },
        checkpoint2,
        { source: "update", step: 2, parents: {} }
      );

      // Get child checkpoint and verify parent relationship
      const childTuple = await saver.getTuple({ configurable: { thread_id: threadId } });
      expect(childTuple?.parentConfig?.configurable?.checkpoint_id).toBe(checkpoint1.id);
    });
  });
});
