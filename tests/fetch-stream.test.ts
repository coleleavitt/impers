import { jest } from "@jest/globals";
import { fetch, Session, SessionClosed } from "../src/index.js";
import { Curl } from "../src/core/easy.js";
import { createStreamGate, getRouteHits, resetRouteHits } from "./mock-server.js";

const url = (path: string): string => `${globalThis.TEST_SERVER_URL}${path}`;

describe("streaming responses", () => {
  test("resolves after headers and before gated body", async () => {
    const gate = createStreamGate("headers-first");
    const responsePromise = fetch(url("/stream-gated/headers-first?body=released"));
    await gate.headers;
    const response = await responsePromise;
    gate.release();
    expect(await response.text()).toBe("released");
  });


  test("selects final headers after informational blocks", async () => {
    const response = await fetch(url("/stream-early-hints"));
    expect(response.status).toBe(200);
    expect(response.headers.get("x-final")).toBe("yes");
    expect(await response.text()).toBe("final body");
  });

  test("does not select an authentication challenge as the response", async () => {
    const session = new Session();
    try {
      const response = await session.stream("GET", url("/stream-auth-negotiation"), {
        auth: { type: "digest", username: "user", password: "password" },
      });
      expect(response.statusCode).toBe(200);
      expect(response.headers.get("x-final")).toBe("yes");
      expect(await response.aText()).toBe("authenticated");
    } finally {
      await session.close();
    }
  });

  test("concurrent response closes await exactly-once native cleanup", async () => {
    const gate = createStreamGate("close-settlement");
    const cleanup = jest.spyOn(Curl.prototype, "cleanup");
    const session = new Session();
    try {
      const request = session.stream("GET", url("/stream-gated/close-settlement"));
      await gate.headers;
      const response = await request;
      await Promise.all([response.close(), response.aClose(), response.close()]);
      expect(cleanup).toHaveBeenCalledTimes(1);
    } finally {
      gate.release();
      await session.close();
      cleanup.mockRestore();
    }
  });

  test("global body cancellation waits for native teardown during completion race", async () => {
    const gate = createStreamGate("cancel-cleanup-race");
    const cleanup = jest.spyOn(Curl.prototype, "cleanup");
    try {
      const request = fetch(url("/stream-gated/cancel-cleanup-race"));
      await gate.headers;
      const response = await request;
      gate.release();
      await response.body!.cancel();
      expect(cleanup).toHaveBeenCalledTimes(1);
    } finally {
      gate.release();
      cleanup.mockRestore();
    }
  });

  test("preserves repeated Set-Cookie through global fetch Headers", async () => {
    const response = await fetch(url("/stream-chunks?chunks=1"));
    expect(response.headers.getSetCookie()).toEqual([
      "first=one; Path=/",
      "second=two; Path=/",
    ]);
    await response.arrayBuffer();
  });

  test("resolves empty body at transfer completion", async () => {
    const response = await fetch(url("/stream-empty"));
    expect(response.status).toBe(204);
    expect(response.body).toBeNull();
  });

  test("surfaces post-header failure and permits a subsequent request", async () => {
    const response = await fetch(url("/stream-truncated"));
    await expect(response.text()).rejects.toThrow();
    expect((await fetch(url("/get"))).status).toBe(200);
  });

  test("cancel stops transfer and permits a subsequent request", async () => {
    const gate = createStreamGate("cancel");
    const responsePromise = fetch(url("/stream-gated/cancel"));
    await gate.headers;
    const response = await responsePromise;
    await response.body!.cancel();
    gate.release();
    expect((await fetch(url("/get"))).status).toBe(200);
  });

  test("aborts deterministically before headers", async () => {
    const gate = createStreamGate("abort-before");
    const controller = new AbortController();
    const request = fetch(url("/stream-before-headers/abort-before"), { signal: controller.signal });
    await gate.headers;
    controller.abort(new Error("stop before headers"));
    gate.release();
    await expect(request).rejects.toThrow("stop before headers");
  });

  test("abort racing completion settles once and permits reuse", async () => {
    const gate = createStreamGate("abort-race");
    const controller = new AbortController();
    const request = fetch(url("/stream-gated/abort-race"), { signal: controller.signal });
    await gate.headers;
    const response = await request;
    gate.release();
    controller.abort(new Error("race abort"));
    await expect(response.text()).rejects.toThrow();
    expect((await fetch(url("/get"))).status).toBe(200);
  });

  test("closing one shared-multi Session does not cancel another", async () => {
    const firstGate = createStreamGate("isolated-first");
    const secondGate = createStreamGate("isolated-second");
    const first = new Session();
    const second = new Session();
    try {
      const firstPromise = first.stream("GET", url("/stream-gated/isolated-first"));
      const secondPromise = second.stream("GET", url("/stream-gated/isolated-second?body=second"));
      await Promise.all([firstGate.headers, secondGate.headers]);
      const [firstResponse, secondResponse] = await Promise.all([firstPromise, secondPromise]);
      await first.close();
      firstGate.release();
      secondGate.release();
      await expect(firstResponse.aContent()).rejects.toBeInstanceOf(SessionClosed);
      expect(await secondResponse.aText()).toBe("second");
    } finally {
      firstGate.release();
      secondGate.release();
      await Promise.all([first.close(), second.close()]);
    }
  });

  test("slow consumer triggers bounded pause/resume without duplicate bytes", async () => {
    const session = new Session();
    try {
      const response = await session.stream("GET", url("/stream-chunks?chunks=20&size=4096&delay=0"), {
        streamHighWaterMark: 16384,
      });
      const chunks: Buffer[] = [];
      for await (const chunk of response.iterContent()) {
        chunks.push(chunk);
        await new Promise<void>((resolve) => setImmediate(resolve));
      }
      const body = Buffer.concat(chunks);
      expect(body).toHaveLength(20 * 4096);
      for (let index = 0; index < 20; index += 1) {
        expect(body.subarray(index * 4096, (index + 1) * 4096)).toEqual(Buffer.alloc(4096, 65 + index));
      }
    } finally {
      await session.close();
    }
  });

  test("manual redirect never hits its target", async () => {
    resetRouteHits("/counted-target");
    const response = await fetch(url("/manual-redirect"), { redirect: "manual" });
    expect(response.status).toBe(302);
    await response.arrayBuffer();
    expect(getRouteHits("/counted-target")).toBe(0);
  });

  test.each([
    ["CRLF", "value\r\nx-injected: yes"],
    ["NUL", "value\0suffix"],
  ])("rejects outbound header %s", async (_label, value) => {
    await expect(fetch(url("/get"), { headers: { "x-test": value } })).rejects.toThrow("Invalid header");
  });

  test("callback exception cleans up and permits a subsequent request", async () => {
    const session = new Session();
    try {
      const response = await session.stream("GET", url("/stream-chunks?chunks=2&delay=0"), {
        contentCallback: () => { throw new Error("callback failed"); },
      });
      await expect(response.aContent()).rejects.toThrow("callback failed");
      expect((await session.get(url("/get"))).statusCode).toBe(200);
    } finally {
      await session.close();
    }
  });

  test("concurrent streams on shared multi complete independently", async () => {
    const sessions = Array.from({ length: 8 }, () => new Session());
    try {
      const responses = await Promise.all(sessions.map((session, index) => session.stream(
        "GET", url(`/stream-chunks?chunks=4&size=${1024 + index}&delay=0`),
      )));
      const bodies = await Promise.all(responses.map((response) => response.aContent()));
      bodies.forEach((body, index) => expect(body).toHaveLength(4 * (1024 + index)));
    } finally {
      await Promise.all(sessions.map((session) => session.close()));
    }
  });

  test("rejects a bound smaller than libcurl maximum callback chunk", async () => {
    const session = new Session();
    try {
      await expect(session.stream("GET", url("/get"), { streamHighWaterMark: 1024 }))
        .rejects.toThrow("at least 16384");
    } finally {
      await session.close();
    }
  });
});
