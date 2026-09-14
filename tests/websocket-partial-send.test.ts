import { jest } from "@jest/globals";
import { createHash } from "node:crypto";
import { createServer, type Server } from "node:http";
import type { AddressInfo, Socket } from "node:net";
import { wsConnect } from "../src/websocket/websocket.js";

jest.setTimeout(30_000);

let server: Server;
let socket: Socket;
let port: number;
let receivedPayload: Promise<Buffer>;
let resolveReceived: (payload: Buffer) => void;

beforeAll(async () => {
  server = createServer();
  server.on("upgrade", (request, upgraded) => {
    socket = upgraded;
    const wireChunks: Buffer[] = [];
    let wireLength = 0;
    let expectedLength: number | undefined;
    upgraded.on("data", (chunk: Buffer) => {
      wireChunks.push(chunk);
      wireLength += chunk.length;
      if (expectedLength === undefined && wireLength >= 10) {
        const prefix = Buffer.concat(wireChunks, wireLength);
        expectedLength = 14 + Number(prefix.readBigUInt64BE(2));
      }
      if (expectedLength === undefined || wireLength < expectedLength) return;
      const wire = Buffer.concat(wireChunks, wireLength);
      const mask = wire.subarray(10, 14);
      const payload = Buffer.from(wire.subarray(14, expectedLength));
      for (let index = 0; index < payload.length; index++) {
        payload[index] ^= mask[index % 4]!;
      }
      resolveReceived(payload);
    });
    const accept = createHash("sha1")
      .update(`${request.headers["sec-websocket-key"]}258EAFA5-E914-47DA-95CA-C5AB0DC85B11`)
      .digest("base64");
    upgraded.write(
      "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n" +
        `Sec-WebSocket-Accept: ${accept}\r\n\r\n`
    );
    upgraded.pause();
  });
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  port = (server.address() as AddressInfo).port;
});

afterAll(async () => {
  socket?.destroy();
  await new Promise<void>((resolve) => server.close(() => resolve()));
});

it("continues a frame after the peer socket becomes writable", async () => {
  receivedPayload = new Promise((resolve) => { resolveReceived = resolve; });
  const ws = await wsConnect(`ws://127.0.0.1:${port}/`);
  const payload = Buffer.alloc(16 * 1024 * 1024, 0xa5);
  let settled = false;
  const sending = ws.send(payload).finally(() => { settled = true; });

  await new Promise((resolve) => setTimeout(resolve, 25));
  expect(settled).toBe(false);
  socket.resume();
  await expect(sending).resolves.toBeUndefined();
  await expect(receivedPayload).resolves.toEqual(payload);
  await ws.close();
});
