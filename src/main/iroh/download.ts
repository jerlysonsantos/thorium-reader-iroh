// ==LICENSE-BEGIN==
// Copyright 2017 European Digital Reading Lab. All rights reserved.
// Licensed to the Readium Foundation under one or more contributor license agreements.
// Use of this source code is governed by a BSD-style license
// that can be found in the LICENSE file exposed on Github (readium) in the project repository.
// ==LICENSE-END==

import debug_ from "debug";
import * as fs from "fs";
import { app } from "electron";
import path from "path";
import { nanoid } from "nanoid";

import { Iroh, BlobTicket, BlobDownloadOptions, SetTagOption, DownloadProgress, NodeAddr, BlobFormat } from "@number0/iroh";
import { gossipManager } from "./gossip";

const debug = debug_("readium-desktop:main:iroh:download");

// ---------------------------------------------------------------------------
// Peer diagnostics helpers
// ---------------------------------------------------------------------------

/** Extracts the IP part from a socket-address string like "192.168.1.5:11204" or "[::1]:11204". */
function extractIp(socketAddr: string): string {
    if (socketAddr.startsWith("[")) {
        // IPv6 bracket notation: [2001:db8::1]:port
        return socketAddr.slice(1, socketAddr.indexOf("]"));
    }
    // IPv4 or bare IPv6: take everything before the last colon
    const lastColon = socketAddr.lastIndexOf(":");
    return lastColon > 0 ? socketAddr.slice(0, lastColon) : socketAddr;
}

/** Returns true if the address is in a private / link-local range. */
function isLocalIp(socketAddr: string): boolean {
    const ip = extractIp(socketAddr);
    return [
        /^10\./,                          // RFC 1918 class A
        /^172\.(1[6-9]|2\d|3[01])\./,    // RFC 1918 class B
        /^192\.168\./,                    // RFC 1918 class C
        /^127\./,                         // loopback
        /^169\.254\./,                    // link-local (APIPA)
        /^::1$/,                          // IPv6 loopback
        /^fc[0-9a-f]{2}:/i,              // IPv6 ULA fc00::/7
        /^fd[0-9a-f]{2}:/i,
    ].some((r) => r.test(ip));
}

/**
 * Logs the peers announced in the ticket and returns a human-readable
 * network classification ("local" | "internet" | "relay-only").
 */
function logTicketPeers(nodeAddr: NodeAddr): void {
    // The NAPI shape may be flat or nested – probe both.
    const a = nodeAddr ;
    const nodeId = a?.nodeId ?? "(unknown)";

    // Direct addresses can be under several keys depending on binding version
    const rawAddrs = a?.addresses
    const relayUrl = a?.relayUrl

    debug("┌─ peers in ticket: 1");
    debug("│  nodeId  :", nodeId);
    debug("│  addrs   :", rawAddrs.length ? rawAddrs.join(", ") : "(none)");
    debug("│  relay   :", relayUrl ?? "(none)");

    if (rawAddrs.length > 0) {
        const hasLocal = rawAddrs.some(isLocalIp);
        const hasInternet = rawAddrs.some((a) => !isLocalIp(a));
        const net = hasLocal && hasInternet ? "local + internet"
            : hasLocal ? "local"
            : "internet";
        debug("│  network :", net);
    } else if (relayUrl) {
        debug("│  network : internet (relay-only, no direct addrs)");
    }
    debug("└─────────────────────────────");
}


// function humanFileSize(bytes: number): string {
//     const thresh = 1024;
//     if (bytes < thresh) return bytes + " B";
//     const units = ["KiB", "MiB", "GiB", "TiB"];
//     let u = -1;
//     do { bytes /= thresh; u++; } while (bytes >= thresh && u < units.length - 1);
//     return bytes.toFixed(1) + " " + units[u];
// }

// Detects the file extension from magic bytes so importFromFsService gets the
// correct extension (it rejects files with unknown extensions).
async function detectExtFromMagicBytes(filePath: string): Promise<string> {
    const buf = Buffer.alloc(8);
    const handle = await fs.promises.open(filePath, "r");
    try {
        await handle.read(buf, 0, 8, 0);
    } finally {
        await handle.close();
    }
    // %PDF
    if (buf[0] === 0x25 && buf[1] === 0x50 && buf[2] === 0x44 && buf[3] === 0x46) return ".pdf";
    // PK zip (EPUB, audiobook, divina …)
    if (buf[0] === 0x50 && buf[1] === 0x4B && buf[2] === 0x03 && buf[3] === 0x04) return ".epub";
    // JSON opening brace → LCP license or webpub manifest
    if (buf[0] === 0x7B) return ".lcpl";
    return ".epub";
}

// Strips characters that are illegal in file names across all major OSes.
export function sanitizeFilename(name: string): string {
    return name.replace(/[/\\:*?"<>|]/g, "_").trim().slice(0, 200) || "download";
}

// ---------------------------------------------------------------------------
// Core download helper — fetches a blob from a specific list of peers.
// ---------------------------------------------------------------------------

/**
 * Low-level: download a blob hash from a set of peer addresses using an
 * ephemeral in-memory node.  Callers are responsible for saving the result.
 */
async function _fetchBlob(
    hash: string,
    format: BlobFormat,
    peers: NodeAddr[],
    onProgress?: (pct: number, humanSize: string) => void,
): Promise<{ node: Awaited<ReturnType<typeof Iroh.memory>>; tmpPath: string }> {

    debug("starting ephemeral node for download, hash:", hash, "peers:", peers.length);
    const node = await Iroh.memory();

    try {
        const opts = new BlobDownloadOptions(format, peers, SetTagOption.auto());

        let totalBytes = BigInt(0);
        await new Promise<void>((resolve, reject) => {
            node.blobs.download(
                hash,
                opts,
                (err: Error | null, event: DownloadProgress) => {
                    if (err) return reject(err);

                    if (event?.found?.size != null) {
                        totalBytes = BigInt(event.found.size);
                    }
                    if (event?.progress?.offset != null && totalBytes > BigInt(0)) {
                        const offset = BigInt(event.progress.offset);
                        const pct = Math.min(99, Number(offset * BigInt(100) / totalBytes));
                        onProgress?.(pct, "");
                    }
                    if (event?.allDone != null) resolve();
                },
            );
        });

        const tmpPath = path.join(app.getPath("temp"), `${nanoid(5)}.tmp`);
        await node.blobs.writeToPath(hash, tmpPath);

        return { node, tmpPath };
    } catch (e) {
        await node.node.shutdown().catch(() => { /* ignore shutdown errors */ });
        throw e;
    }
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/**
 * Downloads a blob from the IROH P2P network using a BlobTicket string.
 *
 * Strategy:
 *  1. Try the peers listed in the ticket directly.
 *  2. If that fails (or the ticket peer list is empty), query the gossip swarm
 *     for additional peers that hold the same blob hash.
 *  3. Seed the downloaded file into the local persistent node and announce it
 *     via gossip so that future downloaders can discover us.
 *
 * Uses an ephemeral in-memory node for the actual download; gossip/seeding
 * uses the persistent node managed by `irohNodeManager`.
 *
 * `onProgress` is called with (0, "") immediately for an indeterminate bar,
 * then with [1–99] as real byte-level progress events arrive from IROH.
 */
export async function downloadBlobFromTicket(
    ticketStr: string,
    title: string,
    onProgress?: (pct: number, humanSize: string) => void,
): Promise<string> {
    onProgress?.(0, "");

    debug("parsing ticket");
    const ticket = BlobTicket.fromString(ticketStr);

    let tmpPath: string;
    let ephemeralNode: Awaited<ReturnType<typeof Iroh.memory>>;

    // --- Attempt 1: use the ticket's own peer address ----------------------
    try {
        const result = await _fetchBlob(
            ticket.hash,
            ticket.format,
            [ticket.nodeAddr],
            onProgress,
        );
        tmpPath = result.tmpPath;
        ephemeralNode = result.node;
        debug("download succeeded via ticket peer");


        logTicketPeers(ticket.nodeAddr);
    } catch (primaryErr) {
        debug("ticket peer failed:", primaryErr, "— querying gossip for fallback peers");

        // --- Attempt 2: gossip peer discovery ------------------------------
        const gossipPeers = await gossipManager.discoverPeers(
            ticket.hash,
            [ticket.nodeAddr.nodeId], // use the ticket nodeId as bootstrap hint
        );

        if (gossipPeers.length === 0) {
            debug("gossip returned no peers — re-throwing primary error");
            throw primaryErr;
        }

        debug("gossip found", gossipPeers.length, "peer(s) — retrying download");

        // Build NodeAddr objects from gossip HAVE payloads.
        const fallbackAddrs: NodeAddr[] = gossipPeers.map((p) => ({
            nodeId: p.nodeId,
            relayUrl: p.relayUrl ?? undefined,
            addresses: [] as Array<string>,
        }));

        const result = await _fetchBlob(
            ticket.hash,
            ticket.format,
            fallbackAddrs,
            onProgress,
        );
        tmpPath = result.tmpPath;
        ephemeralNode = result.node;
        debug("download succeeded via gossip peer(s)");

        // Log the peers the ticket encodes so we can see network topology.
        logTicketPeers(ticket.nodeAddr);
    }

    // Shutdown the ephemeral node — we no longer need it.
    await ephemeralNode.node.shutdown().catch((e) =>
        debug("ephemeral node shutdown error:", e));

    const ext = await detectExtFromMagicBytes(tmpPath);
    const downloadPath = path.join(app.getPath("temp"), `${sanitizeFilename(title)}${ext}`);
    await fs.promises.rename(tmpPath, downloadPath);

    debug("blob saved to", downloadPath);

    // Announce via gossip that we now hold this blob, so we become a fallback
    // peer for future downloaders even after the original seeder goes offline.
    gossipManager.announceBlob(ticket.hash).catch((e) =>
        debug("post-download gossip announce failed:", e));

    return downloadPath;
}
