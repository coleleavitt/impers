/**
 * WebSocket example
 *
 * Start the local echo server first:
 *   npx tsx examples/ws-server.ts
 *
 * Then run this example:
 *   npx tsx examples/websocket.ts
 */
import { wsConnect } from "impers";

async function main() {
  const port = process.argv[2] || "8765";
  const url = `ws://127.0.0.1:${port}/`;

  console.log("Connecting to WebSocket server...");

  try {
    const ws = await wsConnect(url);

    console.log("Connected!");
    console.log("URL:", ws.url);

    // Send a text message
    console.log("\nSending text message...");
    await ws.sendStr("Hello, WebSocket!");

    // Receive the echo
    const response = await ws.recvStr(5);
    console.log("Received:", response);

    // Send JSON
    console.log("\nSending JSON...");
    await ws.sendJson({ type: "greeting", message: "Hello from impers!" });

    // Receive JSON echo
    const jsonResponse = await ws.recvJson<{ type: string; message: string }>(5);
    console.log("Received JSON:", jsonResponse);

    // Close the connection
    console.log("\nClosing connection...");
    await ws.close(1000, "Done");

    console.log("Connection closed.");
  } catch (error) {
    console.error("Error:", error);
  }
}

main().catch(console.error);
