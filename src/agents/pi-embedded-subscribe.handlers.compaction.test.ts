import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import {
  drainSessionStoreLockQueuesForTest,
  resetSessionStoreLockRuntimeForTests,
  setSessionWriteLockAcquirerForTests,
} from "../config/sessions.js";
import { emitAgentEvent } from "../infra/agent-events.js";
import {
  readCompactionCount,
  seedSessionStore,
  waitForCompactionCount,
} from "./pi-embedded-subscribe.compaction-test-helpers.js";
import {
  handleCompactionEnd,
  reconcileSessionStoreCompactionCountAfterSuccess,
} from "./pi-embedded-subscribe.handlers.compaction.js";
import type { EmbeddedPiSubscribeContext } from "./pi-embedded-subscribe.handlers.types.js";

vi.mock("../infra/agent-events.js", () => ({
  emitAgentEvent: vi.fn(),
}));

function createCompactionContext(params: {
  storePath: string;
  sessionKey: string;
  agentId?: string;
  initialCount: number;
  onAgentEvent?: (event: unknown) => void;
}): EmbeddedPiSubscribeContext {
  let compactionCount = params.initialCount;
  return {
    params: {
      runId: "run-test",
      session: { messages: [] } as never,
      config: { session: { store: params.storePath } } as never,
      sessionKey: params.sessionKey,
      sessionId: "session-1",
      agentId: params.agentId ?? "test-agent",
      onAgentEvent: params.onAgentEvent,
    },
    state: {
      compactionInFlight: true,
      pendingCompactionRetry: 0,
    } as never,
    log: {
      debug: vi.fn(),
      warn: vi.fn(),
    },
    ensureCompactionPromise: vi.fn(),
    noteCompactionRetry: vi.fn(),
    maybeResolveCompactionWait: vi.fn(),
    resolveCompactionRetry: vi.fn(),
    resetForCompactionRetry: vi.fn(),
    incrementCompactionCount: () => {
      compactionCount += 1;
    },
    getCompactionCount: () => compactionCount,
  } as unknown as EmbeddedPiSubscribeContext;
}

beforeEach(() => {
  vi.mocked(emitAgentEvent).mockClear();
  setSessionWriteLockAcquirerForTests(async () => ({
    release: async () => {},
  }));
});

afterEach(async () => {
  resetSessionStoreLockRuntimeForTests();
  await drainSessionStoreLockQueuesForTest();
});

describe("reconcileSessionStoreCompactionCountAfterSuccess", () => {
  it("raises the stored compaction count to the observed value", async () => {
    const tmp = await fs.mkdtemp(path.join(os.tmpdir(), "openclaw-compaction-reconcile-"));
    const storePath = path.join(tmp, "sessions.json");
    const sessionKey = "main";
    await seedSessionStore({
      storePath,
      sessionKey,
      compactionCount: 1,
    });

    const nextCount = await reconcileSessionStoreCompactionCountAfterSuccess({
      sessionKey,
      agentId: "test-agent",
      configStore: storePath,
      observedCompactionCount: 2,
      now: 2_000,
    });

    expect(nextCount).toBe(2);
    expect(await readCompactionCount(storePath, sessionKey)).toBe(2);
  });

  it("does not double count when the store is already at or above the observed value", async () => {
    const tmp = await fs.mkdtemp(path.join(os.tmpdir(), "openclaw-compaction-idempotent-"));
    const storePath = path.join(tmp, "sessions.json");
    const sessionKey = "main";
    await seedSessionStore({
      storePath,
      sessionKey,
      compactionCount: 3,
    });

    const nextCount = await reconcileSessionStoreCompactionCountAfterSuccess({
      sessionKey,
      agentId: "test-agent",
      configStore: storePath,
      observedCompactionCount: 2,
      now: 2_000,
    });

    expect(nextCount).toBe(3);
    expect(await readCompactionCount(storePath, sessionKey)).toBe(3);
  });
});

describe("handleCompactionEnd", () => {
  it("reconciles the session store after a successful compaction end event", async () => {
    const tmp = await fs.mkdtemp(path.join(os.tmpdir(), "openclaw-compaction-handler-"));
    const storePath = path.join(tmp, "sessions.json");
    const sessionKey = "main";
    await seedSessionStore({
      storePath,
      sessionKey,
      compactionCount: 1,
    });

    const ctx = createCompactionContext({
      storePath,
      sessionKey,
      initialCount: 1,
    });

    handleCompactionEnd(ctx, {
      type: "compaction_end",
      result: { kept: 12 },
      willRetry: false,
      aborted: false,
    } as never);

    await waitForCompactionCount({
      storePath,
      sessionKey,
      expected: 2,
    });

    expect(await readCompactionCount(storePath, sessionKey)).toBe(2);
  });

  it("emits successful compaction state without token metadata", async () => {
    const tmp = await fs.mkdtemp(path.join(os.tmpdir(), "openclaw-compaction-metadata-"));
    const storePath = path.join(tmp, "sessions.json");
    const sessionKey = "main";
    const onAgentEvent = vi.fn();
    await seedSessionStore({
      storePath,
      sessionKey,
      compactionCount: 0,
    });

    const ctx = createCompactionContext({
      storePath,
      sessionKey,
      initialCount: 0,
      onAgentEvent,
    });

    handleCompactionEnd(ctx, {
      type: "compaction_end",
      result: {
        summary: "compressed summary",
        tokensBefore: 120_000,
        tokensAfter: 45_000,
      },
      willRetry: false,
      aborted: false,
    } as never);

    const expectedData = {
      phase: "end",
      willRetry: false,
      completed: true,
      compacted: true,
    };
    expect(emitAgentEvent).toHaveBeenCalledWith({
      runId: "run-test",
      stream: "compaction",
      data: expectedData,
    });
    expect(onAgentEvent).toHaveBeenCalledWith({
      stream: "compaction",
      data: expectedData,
    });
  });

  it("marks retry compaction as completed but not final-compacted without token metadata", async () => {
    const tmp = await fs.mkdtemp(path.join(os.tmpdir(), "openclaw-compaction-retry-metadata-"));
    const storePath = path.join(tmp, "sessions.json");
    const sessionKey = "main";
    const onAgentEvent = vi.fn();
    await seedSessionStore({
      storePath,
      sessionKey,
      compactionCount: 0,
    });

    const ctx = createCompactionContext({
      storePath,
      sessionKey,
      initialCount: 0,
      onAgentEvent,
    });

    handleCompactionEnd(ctx, {
      type: "compaction_end",
      result: {
        summary: "retry summary",
        tokensBefore: 90_000,
        tokensAfter: 30_000,
      },
      willRetry: true,
      aborted: false,
    } as never);

    expect(onAgentEvent).toHaveBeenCalledWith({
      stream: "compaction",
      data: {
        phase: "end",
        willRetry: true,
        completed: true,
        compacted: false,
      },
    });
  });
});
