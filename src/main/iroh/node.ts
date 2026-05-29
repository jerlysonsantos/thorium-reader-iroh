// ==LICENSE-BEGIN==
// Copyright 2017 European Digital Reading Lab. All rights reserved.
// Licensed to the Readium Foundation under one or more contributor license agreements.
// Use of this source code is governed by a BSD-style license
// that can be found in the LICENSE file exposed on Github (readium) in the project repository.
// ==LICENSE-END==

import debug_ from "debug";
import * as fs from "fs";
import { Iroh, Gossip, Net } from "@number0/iroh";

const debug = debug_("readium-desktop:main:iroh:node");

// Minimal shape of the Iroh instance returned by @number0/iroh.
interface IrohInstance {
    blobs: {
        addFromPath: Function;
        share: Function;
    };
    gossip: Gossip;
    net: Net;
    node: {
        shutdown: () => Promise<void>;
    };
}

// Long-lived IROH node that persists blobs on disk so they can be re-served
// to other peers while the app is running.
class IrohNodeManager {

    private instance: IrohInstance | null = null;

    // Start the persistent node. Call once after app.whenReady().
    async start(dataDir: string): Promise<void> {
        if (this.instance) {
            debug("IROH node already running");
            return;
        }
        try {
            // eslint-disable-next-line @typescript-eslint/no-var-requires
            await fs.promises.mkdir(dataDir, { recursive: true });
            this.instance = await Iroh.persistent(dataDir) as IrohInstance;
            debug("IROH persistent node started at", dataDir);
        } catch (e) {
            // Non-fatal: P2P seeding is best-effort.
            debug("IROH node failed to start:", e);
            this.instance = null;
        }
    }

    // Stop the node gracefully. Call during app shutdown.
    async stop(): Promise<void> {
        if (!this.instance) return;
        try {
            await this.instance.node.shutdown();
            debug("IROH node stopped");
        } catch (e) {
            debug("IROH node stop error:", e);
        } finally {
            this.instance = null;
        }
    }

    isRunning(): boolean {
        return this.instance !== null;
    }

    // Returns the raw Iroh instance. Callers must check isRunning() first.
    getInstance(): IrohInstance | null {
        return this.instance;
    }
}

export const irohNodeManager = new IrohNodeManager();
