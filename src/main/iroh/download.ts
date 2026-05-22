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

const debug = debug_("readium-desktop:main:iroh:download");

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

// Downloads a blob from the IROH P2P network using a BlobTicket string.
// Uses an ephemeral in-memory node so there are no side effects here —
// seeding the downloaded blob into the persistent node is handled separately
// by seedLocalFile().
export async function downloadBlobFromTicket(ticketStr: string, title: string): Promise<string> {
    // eslint-disable-next-line @typescript-eslint/no-var-requires
    const { Iroh, BlobTicket, BlobDownloadOptions, SetTagOption } = require("@number0/iroh");

    debug("parsing ticket");
    const ticket = BlobTicket.fromString(ticketStr);

    debug("starting ephemeral node for download");
    const node = await Iroh.memory();

    try {
        const opts = new BlobDownloadOptions(ticket.format, [ticket.nodeAddr], SetTagOption.auto());

        debug("downloading from peer", ticket.nodeAddr.nodeId);
        await new Promise<void>((resolve, reject) => {
            node.blobs.download(
                ticket.hash,
                opts,
                (err: Error | null, event: { allDone?: unknown }) => {
                    if (err) return reject(err);
                    if (event?.allDone != null) resolve();
                },
            );
        });

        const tmpPath = path.join(app.getPath("temp"), `${nanoid(5)}.tmp`);
        await node.blobs.writeToPath(ticket.hash, tmpPath);

        const ext = await detectExtFromMagicBytes(tmpPath);
        const downloadPath = path.join(app.getPath("temp"), `${sanitizeFilename(title)}${ext}`);
        await fs.promises.rename(tmpPath, downloadPath);

        debug("blob saved to", downloadPath);
        return downloadPath;
    } finally {
        await node.node.shutdown();
    }
}
