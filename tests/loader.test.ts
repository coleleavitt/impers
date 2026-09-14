import {
  LIBCURL_IMPERSONATE_RELEASE_URL,
  LIBCURL_IMPERSONATE_VERSION,
  resolveLibrary,
  writeExtractedEntries,
} from "../src/ffi/loader.js";
import {
  hasImpersonateSupport,
  loadedLibcurlInfo,
  loadedLibcurlPath,
  type LoadedLibcurlInfo,
} from "../src/public.js";
import { existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

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
        env: { IMPER_CACHE_DIR: cacheRoot, IMPER_DOWNLOAD_LIBCURL: "0" },
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
        env: { IMPER_CACHE_DIR: cacheRoot, IMPER_DOWNLOAD_LIBCURL: "0" },
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
