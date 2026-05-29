// ==LICENSE-BEGIN==
// Copyright 2017 European Digital Reading Lab. All rights reserved.
// Licensed to the Readium Foundation under one or more contributor license agreements.
// Use of this source code is governed by a BSD-style license
// that can be found in the LICENSE file exposed on Github (readium) in the project repository.
// ==LICENSE-END==

import debug_ from "debug";
import { Hash, Message, PublicKey, Sender } from "@number0/iroh";
import { irohNodeManager } from "./node";

const debug = debug_("readium-desktop:main:iroh:gossip");

// ---------------------------------------------------------------------------
// Protocol v1
// ---------------------------------------------------------------------------

const PROTO_VERSION = 1 as const;

/**
 * Broadcast by a node that holds (seeds) the blob.
 * Sent on join and re-sent whenever a new direct neighbor appears.
 */
interface HavePayload {
    v: typeof PROTO_VERSION;
    type: "HAVE";
    nodeId: string;
    hash: string;
    relayUrl?: string | null;
}

/**
 * Broadcast by a node that is looking for the blob.
 * All peers that hold it will reply with HAVE.
 */
interface WantPayload {
    v: typeof PROTO_VERSION;
    type: "WANT";
    hash: string;
}

type GossipPayload = HavePayload | WantPayload;

// ---------------------------------------------------------------------------
// Wire helpers
// ---------------------------------------------------------------------------

/** Hash string (base32 / hex) → 32-byte gossip topic `Array<number>`. */
function hashToTopic(hash: string): Array<number> {
    return Hash.fromString(hash).toBytes();
}

function encodeMsg(payload: GossipPayload): Array<number> {
    return Array.from(Buffer.from(JSON.stringify(payload)));
}

function decodeMsg(content: Array<number>): GossipPayload | null {
    try {
        const obj = JSON.parse(Buffer.from(content).toString("utf8")) as GossipPayload;
        if (obj?.v === PROTO_VERSION && (obj.type === "HAVE" || obj.type === "WANT")) {
            return obj;
        }
        return null;
    } catch {
        return null;
    }
}

// ---------------------------------------------------------------------------
// GossipManager
// ---------------------------------------------------------------------------

interface ActiveSub {
    sender: Sender;
    /** Serialised HAVE for this node+hash — re-broadcast when neighbors join. */
    haveMsg: Array<number>;
}

/**
 * Manages long-lived gossip subscriptions for every blob that this node seeds.
 *
 * Topology:
 *   - When A seeds a blob it calls `announceBlob(hash)`.
 *     GossipManager opens a gossip subscription on the blob's topic and broadcasts HAVE.
 *   - When C wants to find peers for a hash it calls `discoverPeers(hash, bootstrap)`.
 *     It joins the same topic, broadcasts WANT, and collects HAVE replies.
 *   - The "bootstrap" list is the set of nodeIds C already knows about (e.g. from
 *     a previously cached ticket). If the list is empty IROH falls back to its
 *     own discovery layer; C will still join the topic once any peer is reachable.
 *
 * Resilience:
 *   - If A goes offline, B (which downloaded from A) keeps its subscription open
 *     and re-broadcasts HAVE to new neighbors. C can therefore discover B even
 *     though it only knew A's nodeId.
 */
class GossipManager {

    /**
     * Active seeding subscriptions, keyed by blob hash string.
     * Kept open for the lifetime of the app so that any late-joining peer
     * can discover us.
     */
    private readonly subs = new Map<string, ActiveSub>();

    // -----------------------------------------------------------------------
    // Public API
    // -----------------------------------------------------------------------

    /**
     * Announce to the gossip swarm that *this* node holds `hash`.
     *
     * - Opens a subscription on the blob topic (no-op if one exists).
     * - Broadcasts HAVE immediately.
     * - Re-broadcasts HAVE whenever a new direct neighbor appears.
     *
     * No-op when the persistent IROH node is not running.
     */
    async announceBlob(hash: string): Promise<void> {
        if (!irohNodeManager.isRunning()) {
            debug("IROH node not running — skipping gossip announce for", hash);
            return;
        }

        // Idempotent: already seeding this blob.
        if (this.subs.has(hash)) {
            debug("already announcing blob", hash, "— skipping");
            return;
        }

        const node = irohNodeManager.getInstance();
        if (!node) return;

        try {
            const myAddr = await node.net.nodeAddr();

            const haveMsg = encodeMsg({
                v: PROTO_VERSION,
                type: "HAVE",
                nodeId: myAddr.nodeId,
                hash,
                relayUrl: myAddr.relayUrl ?? null,
            });

            const topic = hashToTopic(hash);

            // Bootstrap with every peer the IROH node already knows about
            // (mDNS on LAN, relay on internet). This lets the seeder join any
            // existing gossip swarm for this topic immediately.
            const bootstrapIds = await this._knownPeerIds(node);
            debug("announceBlob bootstrap peers:", bootstrapIds.length);

            const sender = await node.gossip.subscribe(
                topic,
                bootstrapIds,
                (err: Error | null, msg: Message) => {
                    if (err) {
                        debug("gossip error (announce) for", hash, err);
                        return;
                    }
                    this._onSeedMessage(msg, hash, haveMsg);
                },
            );

            this.subs.set(hash, { sender, haveMsg });

            // Announce ourselves to whoever is already on the topic.
            await sender.broadcast(haveMsg);
            debug("HAVE announced for", hash, "nodeId:", myAddr.nodeId);

        } catch (e) {
            debug("announceBlob failed for", hash, e);
        }
    }

    /**
     * Join the gossip topic for `hash`, broadcast a WANT, and collect HAVE
     * responses from peers that hold the blob.
     *
     * @param hash         Blob hash to look up.
     * @param bootstrapIds NodeIds to use as gossip bootstrap (may be empty).
     * @param timeoutMs    Maximum time to wait for replies (default 8 s).
     * @returns            List of discovered peers (may be empty on timeout).
     */
    async discoverPeers(
        hash: string,
        bootstrapIds: string[] = [],
        timeoutMs = 20_000,
    ): Promise<Array<{ nodeId: string; relayUrl?: string | null }>> {

        if (!irohNodeManager.isRunning()) {
            debug("IROH node not running — skipping gossip discover for", hash);
            return [];
        }

        const node = irohNodeManager.getInstance();
        if (!node) return [];

        // Merge explicit bootstrap IDs (e.g. from original ticket, may be offline)
        // with every peer the IROH node already knows (mDNS/relay discovery).
        // This is the key to finding M2 when the original seeder (M0) is gone:
        // M2 appears in remoteInfoList via mDNS on the same LAN, and becomes
        // the bootstrap bridge into the gossip swarm for this hash.
        const knownIds = await this._knownPeerIds(node);
        const allBootstrap = [...new Set([...bootstrapIds, ...knownIds])];

        debug("discovering peers for", hash,
            "bootstrap:", allBootstrap.length,
            "(", bootstrapIds.length, "explicit +", knownIds.length, "via mDNS/relay)");

        const collected: Array<{ nodeId: string; relayUrl?: string | null }> = [];
        let resolveWait!: () => void;
        const waitForPeer = new Promise<void>((r) => { resolveWait = r; });
        const timer = setTimeout(resolveWait, timeoutMs);

        let sender: Sender | null = null;
        try {
            const topic = hashToTopic(hash);
            const wantMsg = encodeMsg({ v: PROTO_VERSION, type: "WANT", hash });

            sender = await node.gossip.subscribe(
                topic,
                allBootstrap,
                (err: Error | null, msg: Message) => {
                    if (err) {
                        debug("gossip error (discover) for", hash, err);
                        clearTimeout(timer);
                        resolveWait();
                        return;
                    }

                    if (msg.joined && msg.joined.length > 0) {
                        debug("joined gossip swarm for", hash, "initial peers:", msg.joined);
                    }

                    if (msg.received?.content) {
                        const payload = decodeMsg(msg.received.content);
                        if (payload?.type === "HAVE" && payload.hash === hash) {
                            debug("peer discovered via gossip:", payload.nodeId);
                            collected.push({ nodeId: payload.nodeId, relayUrl: payload.relayUrl });
                            clearTimeout(timer);
                            resolveWait();
                        }
                    }
                },
            );

            // Broadcast WANT so that all nodes on the topic know we're looking.
            await sender.broadcast(wantMsg);
            debug("WANT broadcast for", hash);

            // Wait until we get at least one HAVE or the timeout fires.
            await waitForPeer;

        } catch (e) {
            debug("discoverPeers failed for", hash, e);
        } finally {
            clearTimeout(timer);
            if (sender) {
                try { await (sender as Sender).close(); } catch { /* ignore */ }
            }
        }

        debug("discover finished for", hash, "found", collected.length, "peer(s)");
        return collected;
    }

    /**
     * Close all active seeding subscriptions.
     * Must be called before `irohNodeManager.stop()` during app shutdown.
     */
    async stopAll(): Promise<void> {
        const entries = Array.from(this.subs.entries());
        this.subs.clear();
        for (const [hash, sub] of entries) {
            try {
                await sub.sender.close();
                debug("closed gossip sub for", hash);
            } catch (e) {
                debug("gossip close error for", hash, e);
            }
        }
    }

    // -----------------------------------------------------------------------
    // Private helpers
    // -----------------------------------------------------------------------

    /**
     * Returns nodeId strings for every peer already known to the IROH node
     * (discovered via mDNS on local network, or via relay on internet).
     *
     * These are used to bootstrap gossip subscriptions so that even when the
     * original ticket's seeder is offline, we can still join a swarm through
     * any other reachable peer that may be subscribed to the same topic.
     */
    private async _knownPeerIds(node: NonNullable<ReturnType<typeof irohNodeManager.getInstance>>): Promise<string[]> {
        try {
            const infos = await node.net.remoteInfoList();
            const ids: string[] = [];
            for (const info of infos) {
                try {
                    ids.push(PublicKey.fromBytes(info.nodeId).toString());
                } catch { /* skip malformed entry */ }
            }
            debug("known peers from remoteInfoList:", ids.length);
            return ids;
        } catch {
            return [];
        }
    }

    /**
     * Message handler for *seeding* subscriptions (HAVE mode).
     *
     * - `neighborUp`: a new direct neighbor joined → re-broadcast HAVE so it
     *   learns about us even if it missed the earlier broadcast.
     * - `received WANT`: a peer is looking for this blob → reply with HAVE.
     */
    private _onSeedMessage(msg: Message, hash: string, haveMsg: Array<number>): void {
        const sub = this.subs.get(hash);
        if (!sub) return;

        if (msg.neighborUp) {
            debug("new neighbor for blob", hash, "—", msg.neighborUp, "— re-broadcasting HAVE");
            sub.sender.broadcast(haveMsg).catch((e) =>
                debug("re-broadcast HAVE failed for", hash, e));
        }

        if (msg.received?.content) {
            const payload = decodeMsg(msg.received.content);
            if (payload?.type === "WANT" && payload.hash === hash) {
                debug("received WANT for", hash, "from", msg.received.deliveredFrom, "— replying HAVE");
                sub.sender.broadcast(haveMsg).catch((e) =>
                    debug("WANT→HAVE reply failed for", hash, e));
            }
        }
    }
}

export const gossipManager = new GossipManager();
