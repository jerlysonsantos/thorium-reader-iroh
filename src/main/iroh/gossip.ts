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

interface HavePayload {
    v: typeof PROTO_VERSION;
    type: "HAVE";
    nodeId: string;
    hash: string;
    relayUrl?: string | null;
}

interface WantPayload {
    v: typeof PROTO_VERSION;
    type: "WANT";
    hash: string;
}

type GossipPayload = HavePayload | WantPayload;

// ---------------------------------------------------------------------------
// Wire helpers
// ---------------------------------------------------------------------------

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

export interface DiscoveredPeer {
    nodeId: string;
    relayUrl?: string | null;
}

interface ActiveSub {
    sender: Sender;
    /** Pre-encoded HAVE for this node+hash. Re-broadcast on neighborUp and WANT. */
    haveMsg: Array<number>;
    /** Our own nodeId — used to filter echoes of our own HAVE from the swarm. */
    myNodeId: string;
    /**
     * One-shot resolvers registered by `discoverPeers` calls.
     * When a HAVE arrives from another peer, the first resolver is popped and called.
     * This lets `discoverPeers` reuse the existing swarm connection instead of
     * opening an isolated new subscription.
     */
    haveListeners: Array<(peer: DiscoveredPeer) => void>;
    /**
     * Peers seen in HAVE messages while the swarm was alive.
     * Keyed by nodeId → relayUrl (may be null).
     *
     * When the original bootstrap peer (e.g. Cargo) goes offline, HyParView may
     * leave M1 and M2 with empty active views.  Storing peers seen *during*
     * the active period lets us force a direct reconnect using their nodeId +
     * relayUrl — bypassing the dead bootstrap node entirely.
     */
    seenPeers: Map<string, string | null>;
    /** Prevents concurrent reconnect attempts. */
    reconnecting: boolean;
}

/**
 * Manages long-lived gossip subscriptions for every blob this node seeds.
 *
 * Each blob gets exactly ONE gossip subscription that handles both roles:
 *
 *   Seeder role  — on neighborUp and on WANT → broadcast HAVE
 *   Discoverer   — on HAVE from remote peer  → notify pending discoverPeers callers
 *
 * Having a single subscription per hash is the critical design choice:
 * when M1 has already joined M2's gossip swarm (via the original seeder as
 * bootstrap while it was online), a new `discoverPeers` call must reuse that
 * live swarm connection rather than opening an isolated second subscription.
 */
class GossipManager {

    private readonly subs = new Map<string, ActiveSub>();

    // -----------------------------------------------------------------------
    // Public API
    // -----------------------------------------------------------------------

    /**
     * Announce to the gossip swarm that this node holds `hash`.
     *
     * Opens a subscription on the blob's topic (no-op if one already exists),
     * then broadcasts HAVE.  Re-broadcasts HAVE whenever a new direct neighbor
     * joins the swarm so late peers learn about us without a full WANT cycle.
     *
     * @param explicitBootstrap  Extra nodeIds (e.g. the original seeder) to
     *   bootstrap with in addition to what the persistent node already knows.
     *   Passing the seeder's nodeId while the seeder is still online ensures
     *   both this node and any previous downloader end up in the same swarm,
     *   so the swarm survives after the seeder goes offline.
     */
    async announceBlob(hash: string, explicitBootstrap: string[] = []): Promise<void> {
        if (!irohNodeManager.isRunning()) {
            debug("IROH node not running — skipping gossip announce for", hash);
            return;
        }

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

            const discoveredIds = await this._knownPeerIds(node);
            const allBootstrap = [...new Set([...explicitBootstrap, ...discoveredIds])];
            debug("announceBlob bootstrap:", allBootstrap.length,
                "(", explicitBootstrap.length, "explicit +", discoveredIds.length, "discovered)");

            const sub: ActiveSub = {
                sender: null as unknown as Sender, // filled below
                haveMsg,
                myNodeId: myAddr.nodeId,
                haveListeners: [],
                seenPeers: new Map(),
                reconnecting: false,
            };

            const sender = await node.gossip.subscribe(
                topic,
                allBootstrap,
                (err: Error | null, msg: Message) => {
                    if (err) { debug("gossip error for", hash, err); return; }
                    this._onMessage(msg, hash);
                },
            );

            sub.sender = sender;
            this.subs.set(hash, sub);

            await sender.broadcast(haveMsg);
            debug("HAVE announced for", hash, "nodeId:", myAddr.nodeId);

        } catch (e) {
            debug("announceBlob failed for", hash, e);
        }
    }

    /**
     * Find peers that hold `hash` via the gossip swarm.
     *
     * Fast path — existing swarm connection:
     *   If `announceBlob` was already called for this hash, this node is already
     *   in the gossip swarm.  We broadcast WANT on that live connection and wait
     *   for a HAVE reply from any swarm member (e.g. M2).  This is the common
     *   case after M1 downloaded from the original seeder and then re-attempts
     *   the download after the seeder went offline.
     *
     * Slow path — new subscription:
     *   If there is no active subscription (e.g. M3 has never seen this blob),
     *   we open a new subscription bootstrapped via the known nodeIds and wait
     *   for HAVE.  This requires at least one bootstrap node to be reachable.
     */
    async discoverPeers(
        hash: string,
        bootstrapIds: string[] = [],
        timeoutMs = 20_000,
    ): Promise<DiscoveredPeer[]> {

        if (!irohNodeManager.isRunning()) {
            debug("IROH node not running — skipping gossip discover for", hash);
            return [];
        }

        const node = irohNodeManager.getInstance();
        if (!node) return [];

        const wantMsg = encodeMsg({ v: PROTO_VERSION, type: "WANT", hash });

        // ── Fast path: reuse the live swarm connection ───────────────────────
        const existingSub = this.subs.get(hash);
        if (existingSub) {
            debug("discoverPeers: reusing existing gossip sub for", hash);

            let peer = await this._waitForHave(existingSub, wantMsg, timeoutMs);

            // If no HAVE arrived, the swarm may have fragmented after the original
            // bootstrap peer (e.g. Cargo) went offline.  Try to reconnect directly
            // to peers we saw while the swarm was alive, then retry the WANT.
            if (!peer && existingSub.seenPeers.size > 0 && !existingSub.reconnecting) {
                debug("fast path timeout — reconnecting with", existingSub.seenPeers.size, "known peer(s)");
                await this._reconnectSub(hash, existingSub);
                peer = await this._waitForHave(existingSub, wantMsg, Math.min(timeoutMs, 10_000));
            }

            debug("discoverPeers (existing sub):", peer ? "found" : "no peer");
            return peer ? [peer] : [];
        }

        // ── Slow path: open a new subscription ───────────────────────────────
        const knownIds = await this._knownPeerIds(node);
        const allBootstrap = [...new Set([...bootstrapIds, ...knownIds])];

        debug("discoverPeers (new sub) bootstrap:", allBootstrap.length,
            "(", bootstrapIds.length, "explicit +", knownIds.length, "via mDNS/relay)");

        const collected: DiscoveredPeer[] = [];
        let resolveWait!: () => void;
        const waitForPeer = new Promise<void>((r) => { resolveWait = r; });
        const timer = setTimeout(resolveWait, timeoutMs);

        let sender: Sender | null = null;
        try {
            const topic = hashToTopic(hash);

            sender = await node.gossip.subscribe(
                topic,
                allBootstrap,
                (err: Error | null, msg: Message) => {
                    if (err) {
                        debug("gossip error (new sub discover) for", hash, err);
                        clearTimeout(timer);
                        resolveWait();
                        return;
                    }
                    if (msg.joined && msg.joined.length > 0) {
                        debug("joined gossip swarm for", hash, "peers:", msg.joined);
                    }
                    if (msg.received?.content) {
                        const payload = decodeMsg(msg.received.content);
                        if (payload?.type === "HAVE" && payload.hash === hash) {
                            debug("peer discovered via new sub:", payload.nodeId);
                            collected.push({ nodeId: payload.nodeId, relayUrl: payload.relayUrl });
                            clearTimeout(timer);
                            resolveWait();
                        }
                    }
                },
            );

            await sender.broadcast(wantMsg);
            debug("WANT broadcast (new sub) for", hash);
            await waitForPeer;

        } catch (e) {
            debug("discoverPeers (new sub) failed for", hash, e);
        } finally {
            clearTimeout(timer);
            if (sender) {
                try { await (sender as Sender).close(); } catch { /* ignore */ }
            }
        }

        debug("discoverPeers (new sub) finished for", hash, "—", collected.length, "peer(s)");
        return collected;
    }

    /** Stop all subscriptions before the IROH node shuts down. */
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
    // Private
    // -----------------------------------------------------------------------

    /**
     * Unified message handler for all subscriptions.
     *
     * Each subscription handles both roles simultaneously:
     *   - Seeder:     neighborUp / received WANT  → broadcast HAVE
     *   - Discoverer: received HAVE from remote    → pop and call haveListeners
     */
    private _onMessage(msg: Message, hash: string): void {
        const sub = this.subs.get(hash);
        if (!sub) return;

        // Successfully joined the swarm — the actual QUIC connection is now live.
        // The WANT sent by discoverPeers just before reconnect may have gone into
        // the void (sent before the connection existed), so re-send it now.
        if (msg.joined && msg.joined.length > 0) {
            debug("joined swarm for", hash.slice(0, 12), "peers:", msg.joined.length);
            sub.sender.broadcast(sub.haveMsg).catch((e) =>
                debug("HAVE on join failed:", e));
            if (sub.haveListeners.length > 0) {
                const wantMsg = encodeMsg({ v: PROTO_VERSION, type: "WANT", hash });
                debug("re-sending WANT on join (", sub.haveListeners.length, "listener(s) pending)");
                sub.sender.broadcast(wantMsg).catch((e) =>
                    debug("WANT re-send on join failed:", e));
            }
        }

        // New direct neighbor — re-announce HAVE and re-send pending WANT.
        if (msg.neighborUp) {
            debug("neighborUp for", hash.slice(0, 12), "—", msg.neighborUp!.slice(0, 12), "— re-broadcasting HAVE");
            sub.sender.broadcast(sub.haveMsg).catch((e) =>
                debug("re-broadcast HAVE failed for", hash, e));
            if (sub.haveListeners.length > 0) {
                const wantMsg = encodeMsg({ v: PROTO_VERSION, type: "WANT", hash });
                debug("re-sending WANT on neighborUp (", sub.haveListeners.length, "listener(s) pending)");
                sub.sender.broadcast(wantMsg).catch((e) =>
                    debug("WANT re-send on neighborUp failed:", e));
            }
        }

        if (!msg.received?.content) return;
        const payload = decodeMsg(msg.received.content);
        if (!payload || payload.hash !== hash) return;

        if (payload.type === "WANT") {
            // Someone is looking for this blob — reply with our address.
            debug("received WANT for", hash, "from", msg.received.deliveredFrom, "— replying HAVE");
            sub.sender.broadcast(sub.haveMsg).catch((e) =>
                debug("WANT→HAVE reply failed for", hash, e));
        }

        if (payload.type === "HAVE" && payload.nodeId !== sub.myNodeId) {
            // Record peer for future reconnect bootstrap.
            sub.seenPeers.set(payload.nodeId, payload.relayUrl ?? null);

            // Notify the first pending discoverPeers caller (if any).
            const listener = sub.haveListeners.shift();
            if (listener) {
                debug("received HAVE for", hash, "from", payload.nodeId, "— notifying listener");
                listener({ nodeId: payload.nodeId, relayUrl: payload.relayUrl });
            }
        }
    }

    /**
     * Wait for the next HAVE on `sub` by broadcasting `wantMsg` and resolving
     * on the first reply or on timeout.
     */
    private _waitForHave(
        sub: ActiveSub,
        wantMsg: Array<number>,
        timeoutMs: number,
    ): Promise<DiscoveredPeer | null> {
        return new Promise((resolve) => {
            const timer = setTimeout(() => resolve(null), timeoutMs);
            sub.haveListeners.push((p) => {
                clearTimeout(timer);
                resolve(p);
            });
            sub.sender.broadcast(wantMsg).catch((e) =>
                debug("WANT broadcast failed:", e));
        });
    }

    /**
     * Close the current sender and re-open the gossip subscription using
     * peers seen during the active swarm period as bootstrap.
     *
     * Called when the fast-path WANT times out, indicating the gossip layer
     * lost connectivity (e.g. the shared bootstrap peer went offline and
     * HyParView did not auto-heal the M1↔M2 link in time).
     */
    private async _reconnectSub(hash: string, sub: ActiveSub): Promise<void> {
        if (sub.reconnecting) return;
        sub.reconnecting = true;

        const node = irohNodeManager.getInstance();
        if (!node) { sub.reconnecting = false; return; }

        try {
            // Register previously-seen peers in IROH's address book so they
            // are reachable via relay even without a direct IP path.
            for (const [nodeId, relayUrl] of sub.seenPeers) {
                await node.net.addNodeAddr({
                    nodeId,
                    relayUrl: relayUrl ?? undefined,
                    addresses: [],
                }).catch(() => { /* best-effort */ });
            }

            const seenIds = Array.from(sub.seenPeers.keys());
            const knownIds = await this._knownPeerIds(node);
            const allBootstrap = [...new Set([...seenIds, ...knownIds])];

            debug("_reconnectSub for", hash.slice(0, 12),
                "bootstrap:", allBootstrap.length,
                "(", seenIds.length, "seen +", knownIds.length, "known)");

            // Close old sender gracefully before replacing it.
            try { await sub.sender.close(); } catch { /* ignore */ }

            const topic = hashToTopic(hash);
            const newSender = await node.gossip.subscribe(
                topic,
                allBootstrap,
                (err: Error | null, msg: Message) => {
                    if (err) { debug("gossip error after reconnect:", err); return; }
                    this._onMessage(msg, hash);
                },
            );

            sub.sender = newSender;
            // Re-announce ourselves so the reconnected swarm knows we have the blob.
            await newSender.broadcast(sub.haveMsg);
            debug("gossip reconnected for", hash.slice(0, 12));
        } catch (e) {
            debug("_reconnectSub failed for", hash.slice(0, 12), e);
        } finally {
            sub.reconnecting = false;
        }
    }

    private async _knownPeerIds(
        node: NonNullable<ReturnType<typeof irohNodeManager.getInstance>>,
    ): Promise<string[]> {
        try {
            const infos = await node.net.remoteInfoList();
            const ids: string[] = [];
            for (const info of infos) {
                try { ids.push(PublicKey.fromBytes(info.nodeId).toString()); }
                catch { /* skip */ }
            }
            debug("known peers from remoteInfoList:", ids.length);
            return ids;
        } catch {
            return [];
        }
    }
}

export const gossipManager = new GossipManager();
