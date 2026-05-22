// ==LICENSE-BEGIN==
// Copyright 2017 European Digital Reading Lab. All rights reserved.
// Licensed to the Readium Foundation under one or more contributor license agreements.
// Use of this source code is governed by a BSD-style license
// that can be found in the LICENSE file exposed on Github (readium) in the project repository.
// ==LICENSE-END==

import debug_ from "debug";
import { irohNodeManager } from "./node";

const debug = debug_("readium-desktop:main:iroh:seed");

// Adds a local file to the persistent IROH node so other peers on the network
// can download it while the app is running.
// Returns the BlobTicket string on success, or null if the node is not running.
export async function seedLocalFile(filePath: string): Promise<string | null> {
    if (!irohNodeManager.isRunning()) {
        debug("IROH node not running, skipping seed for", filePath);
        return null;
    }

    // eslint-disable-next-line @typescript-eslint/no-var-requires
    const { SetTagOption } = require("@number0/iroh");

    const node = irohNodeManager.getInstance();

    try {
        debug("adding to persistent node:", filePath);

        // addFromPath uses a callback-based progress pattern.
        const allDone = await new Promise<{ hash: string; format: string }>((resolve, reject) => {
            node.blobs.addFromPath(
                filePath,
                false, // in_place: keep the original file where it is
                SetTagOption.auto(),
                { wrap: false },
                (err: Error | null, progress: { allDone?: { hash: string; format: string } }) => {
                    if (err) return reject(err);
                    if (progress?.allDone != null) resolve(progress.allDone);
                },
            );
        });

        const ticket = await node.blobs.share(allDone.hash, allDone.format, "RelayAndAddresses");
        const ticketStr = ticket.toString();

        debug("seeding", filePath, "ticket:", ticketStr);
        return ticketStr;
    } catch (e) {
        // Non-fatal: seeding is best-effort.
        debug("seed failed for", filePath, e);
        return null;
    }
}
