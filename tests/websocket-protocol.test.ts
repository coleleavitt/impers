import { createHash } from "node:crypto";
import { createServer, type Server } from "node:http";
import type { AddressInfo, Socket } from "node:net";
import { wsConnect } from "../src/websocket/websocket.js";
import { WebSocketClosed, WebSocketError } from "../src/utils/errors.js";

function frame(opcode: number, payload: Buffer, fin = true): Buffer {
  return Buffer.concat([Buffer.from([(fin ? 0x80 : 0) | opcode, payload.length]), payload]);
}

function accept(request: { headers: Record<string, string | string[] | undefined> }, socket: Socket): void {
  const key = request.headers["sec-websocket-key"];
  const value = Array.isArray(key) ? key[0] : key;
  const hash = createHash("sha1").update(`${value}258EAFA5-E914-47DA-95CA-C5AB0DC85B11`).digest("base64");
  socket.write(`HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ${hash}\r\n\r\n`);
}

let server: Server;
let port: number;
const sockets: Socket[] = [];
const clientFrames: Buffer[] = [];

beforeAll(async () => {
  server = createServer();
  server.on("upgrade", (request, socket) => {
    sockets.push(socket);
    accept(request, socket);
    socket.on("data", (data: Buffer) => clientFrames.push(Buffer.from(data)));
    switch (request.url) {
      case "/fragmented":
        socket.write(frame(1, Buffer.from("hello "), false));
        socket.write(frame(0, Buffer.from("fragmented "), false));
        socket.write(frame(0, Buffer.from("world")));
        break;
      case "/too-large":
        socket.write(frame(2, Buffer.alloc(6, 1), false));
        socket.write(frame(0, Buffer.alloc(6, 2)));
        break;
      case "/empty-close":
        socket.write(frame(8, Buffer.alloc(0)));
        break;
      case "/short-close":
        socket.write(frame(8, Buffer.from([3])));
        break;
      case "/bad-code": {
        const payload = Buffer.alloc(2); payload.writeUInt16BE(1005); socket.write(frame(8, payload));
        break;
      }
      case "/bad-utf8":
        socket.write(frame(8, Buffer.from([0x03, 0xe8, 0xc3, 0x28])));
        break;
      case "/receive-error":
        setImmediate(() => socket.destroy());
        break;
      case "/close-echo":
        socket.on("data", () => socket.write(frame(8, Buffer.from([0x03, 0xe8]))));
        break;
      case "/close-destroy":
        socket.on("data", () => socket.destroy());
        break;
    }
  });
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  port = (server.address() as AddressInfo).port;
});

afterAll(async () => {
  for (const socket of sockets) socket.destroy();
  await new Promise<void>((resolve) => server.close(() => resolve()));
});

beforeEach(() => clientFrames.splice(0));

const url = (path: string): string => `ws://127.0.0.1:${port}${path}`;

describe("WebSocket protocol boundaries", () => {
  it("reassembles protocol fragments", async () => {
    const ws = await wsConnect(url("/fragmented"));
    await expect(ws.recvStr(2)).resolves.toBe("hello fragmented world");
    await ws.close();
  });

  it("enforces maxMessageSize cumulatively across protocol fragments", async () => {
    const ws = await wsConnect(url("/too-large"), { maxMessageSize: 10 });
    await expect(ws.recv(2)).rejects.toThrow(/message exceeds maximum size/);
    expect(ws.connected).toBe(false);
    await expect(ws.sendStr("after error")).rejects.toThrow(WebSocketClosed);
    await ws.close();
  });

  it.each([0, -1, 1.5, Number.MAX_SAFE_INTEGER + 1, Number.NaN])(
    "rejects invalid maxMessageSize %s", async (maxMessageSize) => {
      await expect(wsConnect(url("/fragmented"), { maxMessageSize })).rejects.toThrow(
        /positive safe integer/
      );
    }
  );

  it("preserves empty close payload semantics and echoes it", async () => {
    const ws = await wsConnect(url("/empty-close"));
    await expect(ws.recv(2)).rejects.toThrow(WebSocketClosed);
    expect(ws.closeEvent).toEqual({ code: 1005, reason: "", wasClean: true });
    await new Promise((resolve) => setTimeout(resolve, 25));
    expect(clientFrames.some((entry) => (entry[0]! & 0x0f) === 8 && (entry[1]! & 0x7f) === 0)).toBe(true);
  });

  it.each([
    ["one-byte payload", "/short-close", 1002],
    ["reserved close code", "/bad-code", 1002],
    ["invalid UTF-8 reason", "/bad-utf8", 1007],
  ])("rejects malformed close: %s", async (_label, path, responseCode) => {
    const ws = await wsConnect(url(path));
    await expect(ws.recv(2)).rejects.toThrow(WebSocketClosed);
    expect(ws.closeEvent?.wasClean).toBe(false);
    expect(ws.closeEvent?.code).toBe(1006);
    await new Promise((resolve) => setTimeout(resolve, 25));
    const close = clientFrames.find((entry) => (entry[0]! & 0x0f) === 8);
    expect(close).toBeDefined();
    // client frames: 2-byte header, 4-byte mask, then masked close code
    expect((close![6]! ^ close![2]!) * 256 + (close![7]! ^ close![3]!)).toBe(responseCode);
  });

  it("disconnects and makes cleanup idempotent after a receive transport error", async () => {
    const ws = await wsConnect(url("/receive-error"));
    await expect(ws.recv(2)).rejects.toThrow(WebSocketError);
    expect(ws.connected).toBe(false);
    expect(ws.closed).toBe(true);
    await ws.close();
    await ws.close();
  });

  it("settles a pending receive when locally closed", async () => {
    const ws = await wsConnect(url("/no-close"));
    const receive = ws.recv();
    const close = ws.close(1000, "done");

    const results = await Promise.race([
      Promise.allSettled([receive, close]),
      new Promise<never>((_, reject) =>
        setTimeout(() => reject(new Error("close and receive did not settle")), 1_000)
      ),
    ]);

    expect(results[0]).toMatchObject({ status: "rejected", reason: expect.any(WebSocketClosed) });
    expect(results[1]).toEqual({ status: "fulfilled", value: undefined });
    expect(ws.closeEvent).toEqual({ code: 1000, reason: "done", wasClean: false });
  });

  it("settles a pending receive exactly once across double close and transport error", async () => {
    const ws = await wsConnect(url("/close-destroy"));
    let receiveSettlements = 0;
    const receive = ws.recv().finally(() => { receiveSettlements += 1; });
    const firstClose = ws.close(1000, "done");
    const secondClose = ws.close(1001, "ignored");

    const results = await Promise.race([
      Promise.allSettled([receive, firstClose, secondClose]),
      new Promise<never>((_, reject) =>
        setTimeout(() => reject(new Error("close/error race did not settle")), 1_000)
      ),
    ]);

    expect(results[0]).toMatchObject({ status: "rejected", reason: expect.any(WebSocketClosed) });
    expect(results.slice(1).every((result) => result.status !== "pending")).toBe(true);
    expect(receiveSettlements).toBe(1);
    expect(ws.closed).toBe(true);
  });

  it("reports an unacknowledged local close as unclean and remains idempotent", async () => {
    const ws = await wsConnect(url("/no-close"));
    await ws.close(1000, "done");
    expect(ws.closeEvent).toEqual({ code: 1000, reason: "done", wasClean: false });
    await ws.close();
  });

  it("waits for a peer close before reporting a clean local close", async () => {
    const ws = await wsConnect(url("/close-echo"));
    await ws.close(1000, "done");
    expect(ws.closeEvent?.wasClean).toBe(true);
    await ws.close();
  });
});
