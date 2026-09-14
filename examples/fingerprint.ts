/**
 * TLS/HTTP2 Fingerprinting Example
 *
 * This example demonstrates how to use fingerprinting features to customize
 * the TLS and HTTP/2 characteristics of your requests. This is useful for:
 * - Avoiding bot detection
 * - Testing fingerprint-based blocking
 * - Mimicking specific browser configurations
 *
 * NOTE: Full fingerprinting requires curl-impersonate instead of standard libcurl.
 */

import * as impers from "impers";
import { Session, Curl, type ExtraFingerprint } from "impers";

async function main() {
  console.log("=== TLS/HTTP2 Fingerprinting Examples ===\n");

  // Example 1: Using browser impersonation (easiest method)
  console.log("1. Browser Impersonation:");
  console.log("   The simplest way to get realistic fingerprints.\n");

  try {
    const response1 = await impers.get("https://tls.peet.ws/api/all", {
      impersonate: "chrome124",
    });
    console.log("   Impersonating Chrome 124:");
    const data1 = response1.json() as { tls?: { ja3_hash?: string } };
    console.log(`   JA3 Hash: ${data1.tls?.ja3_hash || "N/A"}\n`);
  } catch {
    console.log("   (Impersonation requires curl-impersonate)\n");
  }

  // Example 2: Using JA3 fingerprint string
  console.log("2. JA3 TLS Fingerprint:");
  console.log("   Manual TLS fingerprint configuration.\n");

  // Chrome 120 JA3 string (example)
  const ja3Chrome =
    "771,4865-4866-4867-49195-49199-49196-49200-52393-52392-49171-49172-156-157-47-53,0-23-65281-10-11-35-16-5-13-18-51-45-43-27-17513,29-23-24,0";

  try {
    const response2 = await impers.get("https://tls.peet.ws/api/all", {
      ja3: ja3Chrome,
    });
    console.log(`   JA3 String: ${ja3Chrome.substring(0, 50)}...`);
    const data2 = response2.json() as { tls?: { ja3_hash?: string } };
    console.log(`   Resulting JA3 Hash: ${data2.tls?.ja3_hash || "N/A"}\n`);
  } catch (e) {
    console.log(`   Error: ${e}\n`);
  }

  // Example 3: Using Akamai HTTP/2 fingerprint
  console.log("3. Akamai HTTP/2 Fingerprint:");
  console.log("   Configure HTTP/2 SETTINGS and pseudo header order.\n");

  // Chrome HTTP/2 fingerprint (example)
  const akamaiChrome = "1:65536;3:1000;4:6291456;6:262144|15663105|0|m,a,s,p";

  try {
    const response3 = await impers.get("https://tls.peet.ws/api/all", {
      akamai: akamaiChrome,
    });
    console.log(`   Akamai String: ${akamaiChrome}`);
    const data3 = response3.json() as { http2?: { akamai_fingerprint_hash?: string } };
    console.log(`   HTTP/2 Fingerprint Hash: ${data3.http2?.akamai_fingerprint_hash || "N/A"}\n`);
  } catch (e) {
    console.log(`   Error: ${e}\n`);
  }

  // Example 4: Using ExtraFingerprint for fine-grained control
  console.log("4. Extra Fingerprint Options:");
  console.log("   Fine-grained TLS and HTTP/2 configuration.\n");

  const extraFp: ExtraFingerprint = {
    // TLS signature algorithms
    tlsSigAlgs: [
      "ecdsa_secp256r1_sha256",
      "rsa_pss_rsae_sha256",
      "rsa_pkcs1_sha256",
    ],
    // Elliptic curves
    tlsSupportedGroups: ["X25519", "P-256", "P-384"],
    // HTTP/2 settings (parameter_id: value)
    http2Settings: {
      1: 65536,   // HEADER_TABLE_SIZE
      3: 1000,    // MAX_CONCURRENT_STREAMS
      4: 6291456, // INITIAL_WINDOW_SIZE
      6: 262144,  // MAX_HEADER_LIST_SIZE
    },
    // HTTP/2 pseudo header order (method, authority, scheme, path)
    http2PseudoHeaderOrder: ["m", "a", "s", "p"],
  };

  try {
    const response4 = await impers.get("https://tls.peet.ws/api/all", {
      extraFp,
    });
    console.log("   Applied custom fingerprint settings");
    const data4 = response4.json() as { tls?: { ja3_hash?: string } };
    console.log(`   JA3 Hash: ${data4.tls?.ja3_hash || "N/A"}\n`);
  } catch (e) {
    console.log(`   Error: ${e}\n`);
  }

  // Example 5: Using Session with fingerprinting
  console.log("5. Session with Fingerprinting:");
  console.log("   Reuse fingerprint settings across multiple requests.\n");

  const session = new Session({
    impersonate: "chrome124",
    // Session-level defaults are merged with request-level options
  });

  try {
    const response5 = await session.get("https://tls.peet.ws/api/all");
    const data5 = response5.json() as { tls?: { ja3_hash?: string } };
    console.log(`   Session JA3 Hash: ${data5.tls?.ja3_hash || "N/A"}`);

    // Override at request level
    const response6 = await session.get("https://tls.peet.ws/api/all", {
      ja3: ja3Chrome, // Override session's impersonate
    });
    const data6 = response6.json() as { tls?: { ja3_hash?: string } };
    console.log(`   Override JA3 Hash: ${data6.tls?.ja3_hash || "N/A"}\n`);
  } catch (e) {
    console.log(`   Error: ${e}\n`);
  } finally {
    await session.close();
  }

  // Example 6: Low-level Curl class with fingerprinting
  console.log("6. Low-level Curl API:");
  console.log("   Direct fingerprint configuration on Curl handle.\n");

  const curl = new Curl();
  try {
    // Check if impersonation is available
    console.log(`   Impersonation support: ${Curl.hasImpersonateSupport()}`);

    // Low-level browser impersonation is available directly on Curl.
    // Prefer the high-level request/session APIs for JA3/Akamai configuration.
    // curl.impersonate("chrome124");

    console.log("   Curl handle is ready for low-level impersonation use\n");
  } finally {
    curl.cleanup();
  }

  console.log("=== Done ===");
}

main().catch(console.error);
