import {
  LIBCURL_IMPERSONATE_RELEASE_URL,
  LIBCURL_IMPERSONATE_VERSION,
  pickAsset,
  resolveLibrary,
  writeExtractedEntries,
} from "../src/ffi/loader.js";
import {
  hasImpersonateSupport,
  loadedLibcurlInfo,
  loadedLibcurlPath,
  type LoadedLibcurlInfo,
} from "../src/public.js";
import {
  existsSync,
  mkdirSync,
  mkdtempSync,
  readFileSync,
  rmSync,
  symlinkSync,
  writeFileSync,
} from "node:fs";
import { createHash } from "node:crypto";
import { tmpdir } from "node:os";
import { join } from "node:path";

const digest = (value: string): string =>
  createHash("sha256").update(value).digest("hex");

describe("libcurl loader", () => {
  it("reports immutable details for the loaded native library", () => {
    const info: Readonly<LoadedLibcurlInfo> = loadedLibcurlInfo;

    expect(Object.isFrozen(info)).toBe(true);
    expect(info).toEqual({
      path: loadedLibcurlPath,
      hasCurlEasyImpersonate: hasImpersonateSupport(),
    });
  });

  it("pins the curl-impersonate release", () => {
    expect(LIBCURL_IMPERSONATE_VERSION).toBe("v2.2.2");
    expect(LIBCURL_IMPERSONATE_RELEASE_URL).toBe(
      "https://api.github.com/repos/lexiforest/curl-impersonate/releases/tags/v2.2.2"
    );
  });


  it("prefers an explicit path over the pinned cache", async () => {
    const cacheRoot = mkdtempSync(join(tmpdir(), "impers-loader-"));
    const explicitPath = join(cacheRoot, "explicit", "libcurl-impersonate.so");
    const cachedPath = join(
      cacheRoot,
      LIBCURL_IMPERSONATE_VERSION,
      "linux-x64",
      "libcurl-impersonate.so"
    );
    mkdirSync(join(cacheRoot, "explicit"), { recursive: true });
    mkdirSync(join(cacheRoot, LIBCURL_IMPERSONATE_VERSION, "linux-x64"), { recursive: true });
    writeFileSync(explicitPath, "explicit");
    writeFileSync(cachedPath, "cached");

    try {
      await expect(resolveLibrary({
        env: {
          IMPER_CACHE_DIR: cacheRoot,
          IMPER_DOWNLOAD_LIBCURL: "0",
          IMPER_LIBCURL_SHA256: digest("cached"),
          LIBCURL_IMPERSONATE_PATH: explicitPath,
        },
        platform: "linux",
        arch: "x64",
        impersonateSearchPaths: [],
      })).resolves.toEqual({ path: explicitPath, isImpersonate: true });
    } finally {
      rmSync(cacheRoot, { recursive: true, force: true });
    }
  });

  it("prefers the pinned cached library over an auto-detected system copy", async () => {
    const root = mkdtempSync(join(tmpdir(), "impers-loader-"));
    const cacheRoot = join(root, "cache");
    const cachedPath = join(
      cacheRoot,
      LIBCURL_IMPERSONATE_VERSION,
      "linux-x64",
      "libcurl-impersonate.so"
    );
    const systemPath = join(root, "system", "libcurl-impersonate.so");
    mkdirSync(join(cacheRoot, LIBCURL_IMPERSONATE_VERSION, "linux-x64"), { recursive: true });
    mkdirSync(join(root, "system"), { recursive: true });
    writeFileSync(cachedPath, "pinned");
    writeFileSync(systemPath, "old system copy");

    try {
      await expect(resolveLibrary({
        env: {
          IMPER_CACHE_DIR: cacheRoot,
          IMPER_DOWNLOAD_LIBCURL: "0",
          IMPER_LIBCURL_SHA256: digest("pinned"),
        },
        platform: "linux",
        arch: "x64",
        impersonateSearchPaths: [systemPath],
      })).resolves.toEqual({ path: cachedPath, isImpersonate: true });
    } finally {
      rmSync(root, { recursive: true, force: true });
    }
  });

  it("honors disabled downloads while still using an existing pinned cache", async () => {
    const cacheRoot = mkdtempSync(join(tmpdir(), "impers-loader-"));
    const cachedPath = join(
      cacheRoot,
      LIBCURL_IMPERSONATE_VERSION,
      "linux-x64",
      "libcurl-impersonate.so"
    );
    mkdirSync(join(cacheRoot, LIBCURL_IMPERSONATE_VERSION, "linux-x64"), { recursive: true });
    writeFileSync(cachedPath, "pinned");

    try {
      await expect(resolveLibrary({
        env: {
          IMPER_CACHE_DIR: cacheRoot,
          IMPER_DOWNLOAD_LIBCURL: "0",
          IMPER_LIBCURL_SHA256: digest("pinned"),
        },
        platform: "linux",
        arch: "x64",
        impersonateSearchPaths: [],
      })).resolves.toEqual({ path: cachedPath, isImpersonate: true });
    } finally {
      rmSync(cacheRoot, { recursive: true, force: true });
    }
  });

  it("does not download when downloads are disabled", async () => {
    const root = mkdtempSync(join(tmpdir(), "impers-loader-"));
    const systemPath = join(root, "system", "libcurl-impersonate.so");
    mkdirSync(join(root, "system"), { recursive: true });
    writeFileSync(systemPath, "system");

    try {
      await expect(resolveLibrary({
        env: {
          IMPER_CACHE_DIR: join(root, "cache"),
          IMPER_DOWNLOAD_LIBCURL: "0",
          IMPER_LIBCURL_RELEASE_URL: "https://invalid.invalid/must-not-be-fetched",
        },
        platform: "linux",
        arch: "x64",
        impersonateSearchPaths: [systemPath],
      })).resolves.toEqual({ path: systemPath, isImpersonate: true });
      expect(existsSync(join(root, "cache", LIBCURL_IMPERSONATE_VERSION))).toBe(false);
    } finally {
      rmSync(root, { recursive: true, force: true });
    }
  });

  it("rejects a cached library with the wrong digest", async () => {
    const cacheRoot = mkdtempSync(join(tmpdir(), "impers-loader-"));
    const cachedPath = join(cacheRoot, LIBCURL_IMPERSONATE_VERSION, "linux-x64", "libcurl-impersonate.so");
    mkdirSync(join(cacheRoot, LIBCURL_IMPERSONATE_VERSION, "linux-x64"), { recursive: true });
    writeFileSync(cachedPath, "tampered");
    try {
      const result = await resolveLibrary({
        env: {
          IMPER_CACHE_DIR: cacheRoot,
          IMPER_DOWNLOAD_LIBCURL: "0",
          IMPER_LIBCURL_SHA256: digest("trusted"),
        },
        platform: "linux",
        arch: "x64",
        impersonateSearchPaths: [],
      });
      expect(result).not.toEqual({ path: cachedPath, isImpersonate: true });
    } finally {
      rmSync(cacheRoot, { recursive: true, force: true });
    }
  });

  it("does not select a foreign-platform or wrong-architecture release asset", () => {
    const assets = [{
      name: "libcurl-impersonate-windows-x86_64.zip",
      browser_download_url: "https://example.invalid/windows.zip",
    }, {
      name: "libcurl-impersonate-linux-aarch64.tar.gz",
      browser_download_url: "https://example.invalid/linux-arm64.tar.gz",
    }];
    expect(pickAsset(assets, "linux", "x64")).toBeNull();
  });

  it("does not match win inside darwin", () => {
    const assets = [{
      name: "libcurl-impersonate-darwin-x86_64.zip",
      browser_download_url: "https://example.invalid/darwin.zip",
    }];
    expect(pickAsset(assets, "win32", "x64")).toBeNull();
  });

  it.each([
    ["linux", "libcurl-impersonate-linux-x86_64.so"],
    ["linux", "libcurl-impersonate-linux-x86_64.so.4.8"],
    ["darwin", "libcurl-impersonate-darwin-x86_64.dylib"],
    ["win32", "libcurl-impersonate-windows-x86_64.dll"],
  ])("accepts a strict %s direct-library asset name", (platform, name) => {
    const asset = { name, browser_download_url: `https://example.invalid/${name}` };
    expect(pickAsset([asset], platform, "x64")).toEqual(asset);
  });

  it.each([
    ["linux", "libcurl-impersonate-linux-x86_64.so.sha256"],
    ["linux", "libcurl-impersonate-linux-x86_64.so.sig"],
    ["linux", "libcurl-impersonate-linux-x86_64.so.4.sig"],
    ["darwin", "libcurl-impersonate-darwin-x86_64.dylib.sha256"],
    ["darwin", "libcurl-impersonate-darwin-x86_64.dylib.sig"],
    ["win32", "libcurl-impersonate-windows-x86_64.dll.sha256"],
    ["win32", "libcurl-impersonate-windows-x86_64.dll.sig"],
  ])("rejects a non-library %s direct asset named %s", (platform, name) => {
    const asset = { name, browser_download_url: `https://example.invalid/${name}` };
    expect(pickAsset([asset], platform, "x64")).toBeNull();
  });

  it.each([
    { name: "../outside/libcurl-impersonate.so", type: "file" as const, data: Buffer.from("bad") },
    { name: "/tmp/libcurl-impersonate.so", type: "file" as const, data: Buffer.from("bad") },
    { name: "lib/libcurl-impersonate.so", type: "symlink" as const, linkName: "../../outside" },
  ])("rejects unsafe archive entry $name", (entry) => {
    const targetDir = mkdtempSync(join(tmpdir(), "impers-loader-"));
    try {
      expect(() => writeExtractedEntries([entry], targetDir, "linux")).toThrow(/Unsafe|escapes/);
    } finally {
      rmSync(targetDir, { recursive: true, force: true });
    }
  });

  it("does not write through a pre-existing symlink ancestor", () => {
    const root = mkdtempSync(join(tmpdir(), "impers-loader-"));
    const targetDir = join(root, "target");
    const outsideDir = join(root, "outside");
    mkdirSync(targetDir);
    mkdirSync(outsideDir);
    symlinkSync(outsideDir, join(targetDir, "lib"), "dir");

    try {
      expect(() => writeExtractedEntries([{
        name: "lib/libcurl-impersonate.so",
        data: Buffer.from("must stay inside"),
        type: "file",
      }], targetDir, "linux")).toThrow(/Unsafe extraction directory component/);
      expect(existsSync(join(outsideDir, "libcurl-impersonate.so"))).toBe(false);
    } finally {
      rmSync(root, { recursive: true, force: true });
    }
  });

  it("rejects a symlink extraction root", () => {
    const root = mkdtempSync(join(tmpdir(), "impers-loader-"));
    const outsideDir = join(root, "outside");
    const targetDir = join(root, "target");
    mkdirSync(outsideDir);
    symlinkSync(outsideDir, targetDir, "dir");

    try {
      expect(() => writeExtractedEntries([{
        name: "libcurl-impersonate.so",
        data: Buffer.from("must stay inside"),
        type: "file",
      }], targetDir, "linux")).toThrow(/Unsafe extraction root/);
      expect(existsSync(join(outsideDir, "libcurl-impersonate.so"))).toBe(false);
    } finally {
      rmSync(root, { recursive: true, force: true });
    }
  });

  it("rejects archive symlink cycles", () => {
    const targetDir = mkdtempSync(join(tmpdir(), "impers-loader-"));
    try {
      expect(() => writeExtractedEntries([
        { name: "lib/a", type: "symlink", linkName: "b" },
        { name: "lib/b", type: "symlink", linkName: "a" },
      ], targetDir, "linux")).toThrow(/cycle/);
    } finally {
      rmSync(targetDir, { recursive: true, force: true });
    }
  });

  it.each(["lib", "bin"])(
    "extracts Windows libraries from the %s directory",
    (directory) => {
      const targetDir = mkdtempSync(join(tmpdir(), "impers-loader-"));
      const contents = Buffer.from("test dll");

      try {
        writeExtractedEntries(
          [{
            name: `${directory}/libcurl-impersonate.dll`,
            data: contents,
            type: "file",
          }],
          targetDir,
          "win32"
        );

        const extractedPath = join(
          targetDir,
          directory,
          "libcurl-impersonate.dll"
        );
        expect(existsSync(extractedPath)).toBe(true);
        expect(readFileSync(extractedPath)).toEqual(contents);
      } finally {
        rmSync(targetDir, { recursive: true, force: true });
      }
    }
  );
});
