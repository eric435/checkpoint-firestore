# checkpoint-firestore

Implementation of a [LangGraph.js](https://github.com/langchain-ai/langgraphjs) CheckpointSaver that uses Firestore.
Based off of [langgraph-checkpoint-mongodb](https://www.npmjs.com/package/@langchain/langgraph-checkpoint-mongodb).

## Usage

```ts
import { initializeApp, cert } from "firebase-admin/app";
import { getFirestore } from "firebase-admin/firestore";
import { FirestoreSaver } from "../index.js";

initializeApp({ credential: cert("PATH TO CERT FILE") });
const db = getFirestore();
const saver = new FirestoreSaver({ db });
const checkpoint = {
  v: 1,
  ts: "2024-07-31T20:14:19.804150+00:00",
  id: "1ef4f797-8335-6428-8001-8a1503f9b875",
  channel_values: {
    my_key: "meow",
    node: "node"
  },
  channel_versions: {
    __start__: 2,
    my_key: 3,
    "start:node": 3,
    node: 3
  },
  versions_seen: {
    __input__: {},
    __start__: {
      __start__: 1
    },
    node: {
      "start:node": 2
    }
  },
  pending_sends: [],
}

// store checkpoint
await saver.putWrites(
  {
    configurable: {
      thread_id: "1",
      checkpoint_ns: "",
      checkpoint_id: checkpoint.id,
    },
  },
  [["Write 1", "Write 2"]],
  "task1"
);

// load checkpoint
await saver.getTuple({
  configurable: { 
    thread_id: "1", 
    checkpoint_ns: "",
    checkpoint_id: checkpoint.id,
  }
});

// list checkpoints
await saver.list({
  configurable: { thread_id: "1" },
});

await client.close();
```
