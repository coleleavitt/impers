/**
 * AsyncWebSocket - WebSocket client using libcurl
 *
 * Provides an async interface for WebSocket communication with support
 * for text, binary, ping/pong frames, and proper close handling.
 */

import { Curl } from "../core/easy.js";
import {
  curl_multi_init,
  curl_multi_add_handle,
  curl_multi_remove_handle,
  curl_multi_perform,
  curl_multi_cleanup,
  curl_multi_info_read,
  curl_ws_recv,
  curl_ws_send,
  type CurlHandle,
  type CurlMultiHandle,
} from "../ffi/libcurl.js";
import { CurlOpt, CurlCode, CurlMCode, CurlWsFlag, CurlWsOpt } from "../ffi/constants.js";
import { WebSocketError, WebSocketClosed, ImpersonateError } from "../utils/errors.js";
import { Headers } from "../http/headers.js";
import { Cookies } from "../http/cookies.js";
import { resolveNativeImpersonateTarget } from "../fingerprints.js";
import type { WebSocketOptions } from "../types/options.js";

/** How long to wait between `curl_multi_perform` calls while the handshake is in progress. */
const CONNECT_POLL_MS = 1;

/** `CURLMSG_DONE` — the only message type curl's multi interface defines. */
const CURLMSG_DONE = 1;

/** Avoid letting an unbounded run of control frames monopolize one receive poll. */
const MAX_CONTROL_FRAMES_PER_POLL = 100;

/** Give a peer a short, bounded opportunity to complete a locally initiated close. */
const CLOSE_SETTLE_MS = 250;

const CLOSE_NO_STATUS = 1005;
const CLOSE_ABNORMAL = 1006;
const CLOSE_PROTOCOL_ERROR = 1002;
const CLOSE_INVALID_PAYLOAD = 1007;

/**
 * WebSocket message types
 */
export enum WebSocketMessageType {
  TEXT = "text",
  BINARY = "binary",
  PING = "ping",
  PONG = "pong",
  CLOSE = "close",
}

/**
 * WebSocket message
 */
export interface WebSocketMessage {
  type: WebSocketMessageType;
  data: Buffer;
}

/**
 * WebSocket close event
 */
export interface WebSocketCloseEvent {
  code: number;
  reason: string;
  wasClean: boolean;
}

// Re-export WebSocketOptions for convenience
export type { WebSocketOptions } from "../types/options.js";

/**
 * AsyncWebSocket - Async WebSocket client
 */
export class AsyncWebSocket {
  private curl: Curl;
  private handle: CurlHandle;

  private _url: string;
  private _connected: boolean = false;
  private _closed: boolean = false;
  private _closeEvent: WebSocketCloseEvent | null = null;

  private receiveBuffer: Buffer;
  private messageQueue: WebSocketMessage[] = [];
  private fragmentedFrame: { data: Buffer; flags: number; received: number } | null = null;
  private fragmentedMessage: { chunks: Buffer[]; flags: number; size: number } | null = null;

  private multi: CurlMultiHandle | null = null;
  private pollTimer: ReturnType<typeof setTimeout> | null = null;
  private pollInterval: number = 10; // ms between polls
  private resourcesReleased = false;
  private handleAddedToMulti = false;

  private maxMessageSize: number;

  /**
   * Create a WebSocket connection
   * Use AsyncWebSocket.connect() for the preferred way to create a connection
   */
  private constructor(url: string, options: WebSocketOptions = {}) {
    this._url = url;
    const maxMessageSize = options.maxMessageSize ?? 64 * 1024 * 1024;
    if (!Number.isSafeInteger(maxMessageSize) || maxMessageSize <= 0) {
      throw new WebSocketError("maxMessageSize must be a positive safe integer");
    }
    this.maxMessageSize = maxMessageSize;
    this.receiveBuffer = Buffer.alloc(Math.min(this.maxMessageSize, 1024 * 1024));

    this.curl = new Curl();
    this.handle = this.curl.getHandle()!;

    try {
      this.curl.setOpt(CurlOpt.URL, url);
      this.curl.setOpt(CurlOpt.CONNECT_ONLY, 2);
      this.curl.setOpt(CurlOpt.WS_OPTIONS, CurlWsOpt.CURLWS_NOAUTOPONG);

      if (typeof options.impersonate === "string") {
        const target = resolveNativeImpersonateTarget(options.impersonate);
        if (!target) {
          throw new ImpersonateError(`Impersonating ${options.impersonate} is not supported`);
        }
        try {
          this.curl.impersonate(target, options.defaultHeaders !== false);
        } catch (error) {
          throw new ImpersonateError(
            `Impersonating ${target} is not supported`,
            error instanceof Error ? error : undefined
          );
        }
      }

      if (options.headers) {
        this.curl.setHeaders(new Headers(options.headers).toCurlHeaders());
      }

      if (options.cookies) {
        const cookieHeader = new Cookies(options.cookies).toCookieHeader();
        if (cookieHeader) this.curl.setOpt(CurlOpt.COOKIE, cookieHeader);
      }

      if (options.proxy) this.curl.setOpt(CurlOpt.PROXY, options.proxy);

      const verify = options.verify === false ? 0 : 1;
      this.curl.setOpt(CurlOpt.SSL_VERIFYPEER, verify);
      this.curl.setOpt(CurlOpt.SSL_VERIFYHOST, options.verify === false ? 0 : 2);

      if (options.timeout) this.curl.setOpt(CurlOpt.TIMEOUT, options.timeout);
    } catch (error) {
      this.cleanupResources();
      throw error;
    }
  }

  /**
   * Connect to a WebSocket server
   */
  static async connect(url: string, options: WebSocketOptions = {}): Promise<AsyncWebSocket> {
    const ws = new AsyncWebSocket(url, options);
    await ws.performConnect();
    return ws;
  }

  /**
   * Perform the WebSocket connection handshake, on the main thread and without blocking it.
   *
   * It is driven with the multi interface rather than `curl_easy_perform`, for two reasons
   * that pull in opposite directions and are both satisfied here.
   *
   * It must not run on a libuv worker thread. That is what Koffi's `.async()` does, and
   * driving libcurl from one leaves per-thread state behind whose pthread TSD destructor
   * lives in the libcurl image. At process exit the worker thread is torn down and
   * `_pthread_tsd_cleanup` calls that destructor after the image has gone, so the process
   * dies with SIGSEGV *after* the script has finished — silently, since the script produced
   * all of its output first. It is not specific to WebSockets: any `curl_easy_perform`
   * issued through `.async()` does it, a plain HTTPS GET included.
   *
   * It must also not block the event loop, or a caller talking to a server in its own
   * process — which is exactly what this repository's tests do — would deadlock.
   *
   * `curl_multi_perform` gives both: it returns as soon as there is nothing to do right
   * now, so the handshake advances across event-loop turns without ever leaving the main
   * thread. The handle stays attached to the multi for the life of the socket; removing it
   * would drop the connection that `CONNECT_ONLY` exists to keep.
   */
  private async performConnect(): Promise<void> {
    try {
      this.multi = curl_multi_init();
      if (!this.multi) throw new WebSocketError("Failed to initialize curl multi handle");
      const addCode = curl_multi_add_handle(this.multi, this.handle);
      if (addCode !== CurlMCode.CURLM_OK) {
        throw new WebSocketError(`Failed to add WS handle to multi: ${addCode}`);
      }
      this.handleAddedToMulti = true;

      let running = 1;
      while (running > 0) {
        const perform = curl_multi_perform(this.multi);
        if (perform.code !== 0) {
          throw new WebSocketError(`WS connect failed with multi code ${perform.code}`);
        }
        running = perform.runningHandles;
        if (running > 0) await new Promise((resolve) => setTimeout(resolve, CONNECT_POLL_MS));
      }

      const code = this.readTransferResult();
      if (code !== CurlCode.CURLE_OK) {
        throw new WebSocketError(`WS connect failed with code ${code}`);
      }
      this._connected = true;
    } catch (error) {
      this._closed = true;
      try {
        this.cleanupResources();
      } catch {
        // Preserve the connection/setup error that caused cleanup.
      }
      if (error instanceof WebSocketError) throw error;
      throw new WebSocketError(`Failed to connect: ${error}`);
    }
  }

  /** The completion code the multi recorded for this transfer, once it stopped running. */
  private readTransferResult(): number {
    if (!this.multi) throw new WebSocketError("WS transfer completed without a multi handle");
    for (;;) {
      const { message } = curl_multi_info_read(this.multi);
      if (!message) throw new WebSocketError("WS transfer completed without CURLMSG_DONE");
      if (message.msg === CURLMSG_DONE) return message.result;
    }
  }

  /** Detach and free the multi handle. Safe to call more than once. */
  private releaseMulti(): void {
    if (!this.multi) return;
    const multi = this.multi;
    this.multi = null;
    let failure: WebSocketError | null = null;

    if (this.handleAddedToMulti) {
      this.handleAddedToMulti = false;
      const removeCode = curl_multi_remove_handle(multi, this.handle);
      if (removeCode !== CurlMCode.CURLM_OK) {
        failure = new WebSocketError(`Failed to remove WS handle from multi: ${removeCode}`);
      }
    }

    const cleanupCode = curl_multi_cleanup(multi);
    if (cleanupCode !== CurlMCode.CURLM_OK && !failure) {
      failure = new WebSocketError(`Failed to clean up WS multi handle: ${cleanupCode}`);
    }
    if (failure) throw failure;
  }

  /** Release all native resources exactly once. */
  private cleanupResources(): void {
    if (this.resourcesReleased) return;
    this.resourcesReleased = true;
    let failure: unknown;
    try {
      this.releaseMulti();
    } catch (error) {
      failure = error;
    } finally {
      this.curl.cleanup();
    }
    if (failure) throw failure;
  }

  /**
   * Get the WebSocket URL
   */
  get url(): string {
    return this._url;
  }

  /**
   * Check if connected
   */
  get connected(): boolean {
    return this._connected && !this._closed;
  }

  /**
   * Check if closed
   */
  get closed(): boolean {
    return this._closed;
  }

  /**
   * Get close event details
   */
  get closeEvent(): WebSocketCloseEvent | null {
    return this._closeEvent;
  }

  /**
   * Try to receive a message (non-blocking)
   * Returns the message if available, null if CURLE_AGAIN
   * Throws on error
   */
  private tryReceive(): WebSocketMessage | null {
    if (this._closed) return null;

    for (let controlFrames = 0; controlFrames < MAX_CONTROL_FRAMES_PER_POLL; controlFrames++) {
      const { code, received, frame } = curl_ws_recv(this.handle, this.receiveBuffer);
      if (code === CurlCode.CURLE_AGAIN) return null;
      if (code !== CurlCode.CURLE_OK) {
        const error = new WebSocketError(`Receive error: ${code}`, code);
        this.failConnection();
        throw error;
      }
      if (received === 0 && frame === null) return null;

      const flags = frame?.flags ?? CurlWsFlag.CURLWS_TEXT;
      const offset = Number(frame?.offset ?? 0n);
      const bytesLeft = Number(frame?.bytesleft ?? 0n);
      const frameLength = offset + received + bytesLeft;
      if (!Number.isSafeInteger(offset) || !Number.isSafeInteger(bytesLeft) ||
          !Number.isSafeInteger(frameLength) || offset < 0 || bytesLeft < 0) {
        this.failConnection();
        throw new WebSocketError("Received invalid WebSocket frame metadata");
      }

      const isControl = (flags & (CurlWsFlag.CURLWS_CLOSE | CurlWsFlag.CURLWS_PING | CurlWsFlag.CURLWS_PONG)) !== 0;
      const accumulated = isControl ? 0 : (this.fragmentedMessage?.size ?? 0);
      if (!isControl &&
          (frameLength > this.maxMessageSize || accumulated + frameLength > this.maxMessageSize)) {
        this.failConnection();
        throw new WebSocketError(`WebSocket message exceeds maximum size of ${this.maxMessageSize} bytes`);
      }

      let data: Buffer;
      let frameFlags = flags;
      if (offset === 0 && bytesLeft === 0) {
        data = Buffer.from(this.receiveBuffer.subarray(0, received));
      } else {
        if (offset === 0) {
          this.fragmentedFrame = { data: Buffer.alloc(frameLength), flags, received: 0 };
        }
        const fragmented = this.fragmentedFrame;
        if (!fragmented || offset !== fragmented.received || frameLength !== fragmented.data.length) {
          this.fragmentedFrame = null;
          this.failConnection();
          throw new WebSocketError("Received inconsistent fragmented WebSocket frame metadata");
        }
        this.receiveBuffer.copy(fragmented.data, offset, 0, received);
        fragmented.received += received;
        if (bytesLeft > 0) return null;
        data = fragmented.data;
        frameFlags = fragmented.flags;
        this.fragmentedFrame = null;
      }

      if (isControl) {
        if ((frameFlags & CurlWsFlag.CURLWS_CONT) !== 0 || data.length > 125) {
          this.abortWithClose(CLOSE_PROTOCOL_ERROR, "Invalid control frame");
          throw new WebSocketClosed(CLOSE_ABNORMAL, "Invalid control frame");
        }
        const message = this.frameToMessage(data, frameFlags);
        if (message?.type === WebSocketMessageType.CLOSE) {
          this.handleCloseFrame(message.data);
          throw new WebSocketClosed(this._closeEvent?.code ?? CLOSE_ABNORMAL, this._closeEvent?.reason ?? "");
        }
        if (message?.type === WebSocketMessageType.PING) {
          try {
            this.sendPongNow(message.data);
          } catch (error) {
            this.failConnection();
            throw error;
          }
        }
        if (message?.type === WebSocketMessageType.PONG) return message;
        continue;
      }

      const continues = (frameFlags & CurlWsFlag.CURLWS_CONT) !== 0;
      const messageTypeFlags = frameFlags & (CurlWsFlag.CURLWS_TEXT | CurlWsFlag.CURLWS_BINARY);
      if (this.fragmentedMessage) {
        if (messageTypeFlags !== this.fragmentedMessage.flags) {
          this.failConnection();
          throw new WebSocketError("Fragmented WebSocket message changed type");
        }
        this.fragmentedMessage.chunks.push(data);
        this.fragmentedMessage.size += data.length;
        if (continues) return null;
        const message = this.frameToMessage(
          Buffer.concat(this.fragmentedMessage.chunks, this.fragmentedMessage.size),
          this.fragmentedMessage.flags
        );
        this.fragmentedMessage = null;
        return message;
      }
      if (continues) {
        this.fragmentedMessage = { chunks: [data], flags: messageTypeFlags, size: data.length };
        return null;
      }
      return this.frameToMessage(data, frameFlags);
    }

    return null;
  }

  /**
   * Convert frame flags to message type
   */
  private frameToMessage(data: Buffer, flags: number): WebSocketMessage | null {
    if (flags & CurlWsFlag.CURLWS_TEXT) {
      return { type: WebSocketMessageType.TEXT, data };
    } else if (flags & CurlWsFlag.CURLWS_BINARY) {
      return { type: WebSocketMessageType.BINARY, data };
    } else if (flags & CurlWsFlag.CURLWS_PING) {
      return { type: WebSocketMessageType.PING, data };
    } else if (flags & CurlWsFlag.CURLWS_PONG) {
      return { type: WebSocketMessageType.PONG, data };
    } else if (flags & CurlWsFlag.CURLWS_CLOSE) {
      return { type: WebSocketMessageType.CLOSE, data };
    }
    // Default to binary
    return { type: WebSocketMessageType.BINARY, data };
  }

  /**
   * Handle close frame
   */
  private handleCloseFrame(data: Buffer): void {
    const parsed = this.parseClosePayload(data);
    if (!parsed.valid) {
      this.abortWithClose(parsed.errorCode, parsed.reason);
      return;
    }

    let echoed = true;
    if (!this._closeEvent) {
      try {
        this.sendClosePayload(data);
      } catch {
        echoed = false;
      }
    }
    this._closeEvent = { code: parsed.code, reason: parsed.reason, wasClean: echoed };
    this._closed = true;
    this._connected = false;
    this.stopPolling();
    this.cleanupResources();
  }

  private parseClosePayload(data: Buffer):
    | { valid: true; code: number; reason: string }
    | { valid: false; errorCode: number; reason: string } {
    if (data.length === 0) return { valid: true, code: CLOSE_NO_STATUS, reason: "" };
    if (data.length === 1 || data.length > 125) {
      return { valid: false, errorCode: CLOSE_PROTOCOL_ERROR, reason: "Invalid close payload" };
    }
    const code = data.readUInt16BE(0);
    if (!this.isValidCloseCode(code)) {
      return { valid: false, errorCode: CLOSE_PROTOCOL_ERROR, reason: "Invalid close code" };
    }
    try {
      const reason = new TextDecoder("utf-8", { fatal: true }).decode(data.subarray(2));
      return { valid: true, code, reason };
    } catch {
      return { valid: false, errorCode: CLOSE_INVALID_PAYLOAD, reason: "Invalid close reason" };
    }
  }

  private isValidCloseCode(code: number): boolean {
    return (code >= 1000 && code <= 1014 && code !== 1004 && code !== 1005 && code !== 1006) ||
      (code >= 3000 && code <= 4999);
  }

  private sendClosePayload(payload: Buffer): void {
    const { code, sent } = curl_ws_send(this.handle, payload, CurlWsFlag.CURLWS_CLOSE);
    if (code !== CurlCode.CURLE_OK || sent !== payload.length) {
      throw new WebSocketError(code !== CurlCode.CURLE_OK
        ? `Close send failed with code ${code}`
        : `Incomplete close send: ${sent}/${payload.length} bytes`, code);
    }
  }

  private abortWithClose(code: number, reason: string): void {
    const reasonBytes = Buffer.from(reason, "utf-8").subarray(0, 123);
    const payload = Buffer.alloc(2 + reasonBytes.length);
    payload.writeUInt16BE(code, 0);
    reasonBytes.copy(payload, 2);
    try { this.sendClosePayload(payload); } catch { /* best effort protocol error */ }
    this._closeEvent = { code: CLOSE_ABNORMAL, reason, wasClean: false };
    this.failConnection();
  }

  private stopPolling(): void {
    if (this.pollTimer) clearTimeout(this.pollTimer);
    this.pollTimer = null;
  }

  private failConnection(): void {
    this._closed = true;
    this._connected = false;
    this.stopPolling();
    try { this.cleanupResources(); } catch { /* preserve the transport/protocol error */ }
  }

  /**
   * Receive a message with polling
   */
  async recv(timeout?: number): Promise<WebSocketMessage> {
    if (this._closed) {
      throw new WebSocketClosed(
        this._closeEvent?.code ?? CLOSE_ABNORMAL,
        this._closeEvent?.reason || "Connection closed"
      );
    }

    // Check queue first
    if (this.messageQueue.length > 0) {
      return this.messageQueue.shift()!;
    }

    const startTime = Date.now();
    const timeoutMs = timeout !== undefined ? timeout * 1000 : Infinity;

    // Poll for message
    return new Promise<WebSocketMessage>((resolve, reject) => {
      let settled = false;

      const poll = () => {
        if (settled) return;

        // Check timeout
        if (Date.now() - startTime >= timeoutMs) {
          settled = true;
          this.pollTimer = null;
          reject(new WebSocketError("Receive timeout"));
          return;
        }

        // Check if closed
        if (this._closed) {
          settled = true;
          this.pollTimer = null;
          reject(
            new WebSocketClosed(
              this._closeEvent?.code ?? CLOSE_ABNORMAL,
              this._closeEvent?.reason || "Connection closed"
            )
          );
          return;
        }

        try {
          const message = this.tryReceive();
          if (message) {
            settled = true;
            this.pollTimer = null;
            resolve(message);
            return;
          }

          // No message, schedule next poll (only if not closed/settled)
          if (!this._closed && !settled) {
            this.pollTimer = setTimeout(poll, this.pollInterval);
          }
        } catch (error) {
          settled = true;
          this.pollTimer = null;
          reject(error);
        }
      };

      // Start polling
      poll();
    });
  }

  /**
   * Receive a text message
   */
  async recvStr(timeout?: number): Promise<string> {
    const msg = await this.recv(timeout);
    return msg.data.toString("utf-8");
  }

  /**
   * Receive and parse as JSON
   */
  async recvJson<T = unknown>(timeout?: number): Promise<T> {
    const str = await this.recvStr(timeout);
    return JSON.parse(str) as T;
  }

  /**
   * Send raw data with flags
   */
  private async sendRaw(data: Buffer, flags: number): Promise<void> {
    if (this._closed) {
      throw new WebSocketClosed(
        this._closeEvent?.code ?? CLOSE_ABNORMAL,
        this._closeEvent?.reason || "Connection closed"
      );
    }

    const { code, sent } = curl_ws_send(this.handle, data, flags);
    if (code !== CurlCode.CURLE_OK || sent !== data.length) {
      const error = new WebSocketError(code !== CurlCode.CURLE_OK
        ? `Send failed with code ${code}`
        : `Incomplete send: ${sent}/${data.length} bytes`, code);
      this.failConnection();
      throw error;
    }
  }

  /**
   * Send binary data
   */
  async send(data: Buffer | Uint8Array): Promise<void> {
    const buffer = Buffer.isBuffer(data) ? data : Buffer.from(data);
    await this.sendRaw(buffer, CurlWsFlag.CURLWS_BINARY);
  }

  /**
   * Send text message
   */
  async sendStr(text: string): Promise<void> {
    const buffer = Buffer.from(text, "utf-8");
    await this.sendRaw(buffer, CurlWsFlag.CURLWS_TEXT);
  }

  /**
   * Send binary data
   */
  async sendBinary(data: Buffer | Uint8Array): Promise<void> {
    await this.send(data);
  }

  /**
   * Send JSON message
   */
  async sendJson(data: unknown): Promise<void> {
    const json = JSON.stringify(data);
    await this.sendStr(json);
  }

  /**
   * Send ping frame
   */
  async ping(data?: Buffer | string): Promise<void> {
    const buffer = data
      ? Buffer.isBuffer(data)
        ? data
        : Buffer.from(data, "utf-8")
      : Buffer.alloc(0);
    await this.sendRaw(buffer, CurlWsFlag.CURLWS_PING);
  }

  /**
   * Send a pong frame.
   *
   * Public because an unsolicited pong is the standard unidirectional heartbeat
   * (RFC 6455 section 5.5.3), and because on this transport it is the only way to keep an
   * otherwise idle connection alive. libcurl answers a server ping by *queueing* a pong,
   * and in CONNECT_ONLY mode nothing is written to the socket until the application sends
   * something — so a consumer that only ever receives never actually delivers a pong, and
   * a server that enforces a pong deadline closes the connection. Any send flushes the
   * queued pong; a pong is the quietest one, since it asks for no reply.
   */
  async pong(data?: Buffer | string): Promise<void> {
    const buffer =
      data === undefined
        ? Buffer.alloc(0)
        : Buffer.isBuffer(data)
          ? data
          : Buffer.from(data, "utf-8");
    await this.sendRaw(buffer, CurlWsFlag.CURLWS_PONG);
  }

  /** Answer a received ping synchronously so failures surface in the active receive. */
  private sendPongNow(data: Buffer): void {
    const { code, sent } = curl_ws_send(this.handle, data, CurlWsFlag.CURLWS_PONG);
    if (code !== CurlCode.CURLE_OK) {
      throw new WebSocketError(`Automatic pong failed with code ${code}`);
    }
    if (sent !== data.length) {
      throw new WebSocketError(`Incomplete automatic pong: ${sent}/${data.length} bytes`);
    }
  }

  /**
   * Close the WebSocket connection
   */
  async close(code: number = 1000, reason: string = ""): Promise<void> {
    if (this._closed) return;
    if (!this.isValidCloseCode(code)) throw new WebSocketError(`Invalid close code: ${code}`);
    this.stopPolling();

    const reasonBytes = Buffer.from(reason, "utf-8");
    if (reasonBytes.length > 123) throw new WebSocketError("Close reason exceeds 123 UTF-8 bytes");
    const payload = Buffer.alloc(2 + reasonBytes.length);
    payload.writeUInt16BE(code, 0);
    reasonBytes.copy(payload, 2);

    try {
      this.sendClosePayload(payload);
    } catch (error) {
      this.failConnection();
      throw error;
    }

    // Record the local proposal but do not report a clean close until a valid peer CLOSE
    // arrives. Continue receiving for a bounded period so the handshake can complete.
    this._closeEvent = { code, reason, wasClean: false };
    const deadline = Date.now() + CLOSE_SETTLE_MS;
    while (!this._closed && Date.now() < deadline) {
      try {
        this.tryReceive();
      } catch (error) {
        if (error instanceof WebSocketClosed) break;
        throw error;
      }
      if (!this._closed) await new Promise((resolve) => setTimeout(resolve, this.pollInterval));
    }

    if (!this._closed) {
      this._closed = true;
      this._connected = false;
      this.stopPolling();
      this.cleanupResources();
    }
  }

  /**
   * Async iterator for receiving messages
   */
  async *[Symbol.asyncIterator](): AsyncIterableIterator<WebSocketMessage> {
    while (!this._closed) {
      try {
        yield await this.recv();
      } catch (error) {
        if (error instanceof WebSocketClosed) {
          return;
        }
        throw error;
      }
    }
  }
}

/**
 * Connect to a WebSocket server
 */
export async function wsConnect(
  url: string,
  options?: WebSocketOptions
): Promise<AsyncWebSocket> {
  return AsyncWebSocket.connect(url, options);
}
