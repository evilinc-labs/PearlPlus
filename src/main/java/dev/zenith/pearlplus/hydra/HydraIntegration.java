package dev.zenith.pearlplus.hydra;

import com.github.rfresh2.EventConsumer;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.rabbitmq.client.*;
import com.zenith.cache.data.chunk.Chunk;
import com.zenith.event.client.ClientBotTick;
import com.zenith.event.client.ClientOnlineEvent;
import com.zenith.module.api.Module;
import dev.zenith.pearlplus.PearlPlusConfig;
import dev.zenith.pearlplus.PearlPlusPlugin;
import dev.zenith.pearlplus.module.PearlManager;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.LinkedBlockingQueue;

import static com.github.rfresh2.EventConsumer.of;
import static com.zenith.Globals.CACHE;
import static dev.zenith.pearlplus.PearlPlusPlugin.*;

/**
 * Optional Hydra C2 integration for PearlPlus.
 *
 * <p>Activates automatically when both {@code HYDRA_RABBIT_URL} and
 * {@code HYDRA_AGENT_ID} environment variables are present. When active:
 * <ul>
 *   <li>Creates a private, ephemeral queue bound to the agent's command routing key so it
 *       receives copies of all commands without competing with hydra-zenith-agent's queue.</li>
 *   <li>Handles {@code PEARL_LOAD} commands — looks up the player's pearls and triggers a load.</li>
 *   <li>Publishes {@code agent.pearl.load} results back to {@code hydra.events}.</li>
 * </ul>
 *
 * <p>When env vars are absent the module registers but does nothing — zero impact on
 * standalone PearlPlus deployments.
 *
 * <h3>Queue Design</h3>
 * <p>This module declares a <em>separate</em> exclusive, auto-delete queue bound to the same
 * routing key ({@code agent.<id>.commands}) as hydra-zenith-agent's command queue. RabbitMQ
 * delivers a copy of each inbound command to every bound queue, so both consumers receive
 * all commands without round-robin competition. Unknown command types are silently ignored.
 *
 * <h3>Thread Model</h3>
 * <p>The RabbitMQ consumer thread enqueues {@link PearlLoadRequest} objects into a
 * {@link LinkedBlockingQueue}. The game tick handler drains the queue on ZenithProxy's
 * game thread before calling {@link PearlManager} methods, which interact with Baritone
 * and the entity cache and must run on the game thread.
 *
 * <h3>RabbitMQ Protocol</h3>
 * <b>Inbound command</b> (exchange: {@code hydra.commands},
 *   routing key: {@code agent.<id>.commands}):
 * <pre>{@code
 * {
 *   "type":      "PEARL_LOAD",
 *   "commandId": "<uuid>",
 *   "data": {
 *     "playerUUID": "<mojang-uuid>",
 *     "playerName": "<mc-username>"
 *   }
 * }
 * }</pre>
 *
 * <b>Outbound event</b> (exchange: {@code hydra.events},
 *   routing key: {@code agent.<id>.agent.pearl.load}):
 * <pre>{@code
 * {
 *   "agentId":   "<id>",
 *   "eventType": "agent.pearl.load",
 *   "ts":        <epoch-ms>,
 *   "data": {
 *     "commandId":  "<uuid>",
 *     "status":     "loading | no_pearls | not_found | error",
 *     "pearlCount": <n>,
 *     "pearlId":    "<id>",
 *     "playerName": "<mc-username>"
 *   }
 * }
 * }</pre>
 *
 * <p>Status semantics:
 * <ul>
 *   <li>{@code loading}   — pearl found; bot is pathfinding to the stasis chamber</li>
 *   <li>{@code no_pearls} — player has no pearls registered at this agent</li>
 *   <li>{@code not_found} — a pearl record exists but the requested ID cannot be resolved</li>
 *   <li>{@code error}     — bad request data (missing UUID, parse failure, etc.)</li>
 * </ul>
 */
public class HydraIntegration extends Module {

    // Exchange names must match hydra-zenith-agent's topology (see RabbitMQManager.java).
    private static final String EXCHANGE_EVENTS   = "hydra.events";
    private static final String EXCHANGE_COMMANDS = "hydra.commands";

    // PearlManager instance owned by this module — separate from AutoLoadModule's instance
    // so Baritone tasks submitted here don't interfere with whisper-driven loads.
    private final PearlManager pearlManager = new PearlManager(this);

    // Load requests flow: RabbitMQ consumer thread → queue → game tick thread.
    private final LinkedBlockingQueue<PearlLoadRequest> pendingLoads = new LinkedBlockingQueue<>();
    // Ledger updates flow: RabbitMQ consumer thread → queue → game tick thread (purge must run on game thread).
    private final LinkedBlockingQueue<List<HydraLedger.LedgerEntry>> pendingLedgerUpdates = new LinkedBlockingQueue<>();

    private volatile Connection rabbitConn;
    private volatile Channel    publishCh;
    private volatile boolean    hydraActive = false;
    private String agentId;

    // Pearl audit: runs on proxy.online and every 5 minutes (6000 ticks).
    private static final int AUDIT_INTERVAL_TICKS = 6000; // ~5 minutes at 20 tps
    private int auditTickCounter = 0;
    private boolean auditOnNextTick = false; // set by proxy.online event

    // Readiness gate for PEARL_LOAD after a fresh login. proxy.online fires the
    // instant JoinGame is processed, but chunks + entity cache populate over
    // the next 1-2 seconds. Defer PEARL_LOAD until either the target pearl's
    // chunk is in cache OR the bot has been online long enough. After
    // PEARL_LOAD_MAX_DEFER_TICKS, give up and process anyway (better to fail
    // loudly than hang forever).
    private static final int PEARL_LOAD_MIN_UPTIME_TICKS = 20;  // ~1s at 20 tps
    private static final int PEARL_LOAD_MAX_DEFER_TICKS  = 60;  // ~3s at 20 tps
    private long onlineSinceMillis = -1L;

    // ── Module lifecycle ────────────────────────────────────────────────────────

    /** Always enabled so ZenithProxy keeps the event subscription alive. */
    @Override
    public boolean enabledSetting() {
        return true;
    }

    @Override
    public List<EventConsumer<?>> registerEvents() {
        return List.of(
            of(ClientBotTick.class, this::onGameTick),
            of(ClientOnlineEvent.class, event -> {
                // Stamp the moment we entered the game world. PEARL_LOADs that
                // arrive within PEARL_LOAD_MIN_UPTIME_TICKS get deferred until
                // chunks settle.
                onlineSinceMillis = System.currentTimeMillis();
            }),
            of(ClientBotTick.Stopped.class, event -> {
                // Schedule an audit on next connect so we reconcile pearl state.
                auditOnNextTick = true;
                auditTickCounter = 0;
                onlineSinceMillis = -1L;
            })
        );
    }

    /**
     * Called from {@link dev.zenith.pearlplus.PearlPlusPlugin#onLoad} after standard modules
     * are registered. Reads env vars and connects to RabbitMQ if both are present.
     */
    public void tryConnect() {
        String rabbitUrl = System.getenv("HYDRA_RABBIT_URL");
        agentId          = System.getenv("HYDRA_AGENT_ID");

        if (rabbitUrl == null || rabbitUrl.isBlank() || agentId == null || agentId.isBlank()) {
            LOG.info("[Hydra] HYDRA_RABBIT_URL / HYDRA_AGENT_ID not set — Hydra integration disabled");
            return;
        }

        try {
            ConnectionFactory factory = new ConnectionFactory();
            factory.setUri(rabbitUrl);
            factory.setConnectionTimeout(5_000);
            factory.setRequestedHeartbeat(30);
            factory.setAutomaticRecoveryEnabled(true); // reconnects on network blips

            rabbitConn = factory.newConnection("pearlplus-hydra");

            // ── Publish channel ─────────────────────────────────────────────────
            publishCh = rabbitConn.createChannel();
            publishCh.exchangeDeclare(EXCHANGE_EVENTS, BuiltinExchangeType.TOPIC, true);

            // ── Command consumer channel ────────────────────────────────────────
            // We declare a private, auto-delete queue bound to the same routing key as
            // hydra-zenith-agent's command queue. RabbitMQ delivers a copy of each command
            // to every bound queue — so PearlPlus sees all commands without competing for
            // messages with the main agent consumer.
            Channel consumeCh = rabbitConn.createChannel();
            consumeCh.exchangeDeclare(EXCHANGE_COMMANDS, BuiltinExchangeType.TOPIC, true);

            String commandRoutingKey = "agent." + agentId + ".commands";
            // Exclusive + auto-delete: this queue exists only while PearlPlus is connected
            // and is automatically removed when the consumer disconnects.
            String pearlQueue = consumeCh.queueDeclare("", false, true, true, null).getQueue();
            consumeCh.queueBind(pearlQueue, EXCHANGE_COMMANDS, commandRoutingKey);
            consumeCh.basicConsume(pearlQueue, /*autoAck=*/true, this::onCommandDelivery, tag -> {});

            hydraActive = true;
            LOG.info("[Hydra] Connected to RabbitMQ — agent ID: {}, listening for PEARL_LOAD commands", agentId);
        } catch (Exception e) {
            LOG.warn("[Hydra] Failed to connect to RabbitMQ: {} — pearl C2 commands unavailable", e.getMessage());
        }
    }

    // ── RabbitMQ consumer (RabbitMQ consumer thread) ────────────────────────────

    /**
     * Receives a raw command delivery from the {@code hydra.commands} exchange.
     * Only {@code PEARL_LOAD} commands are enqueued; everything else is silently dropped
     * (they are intended for hydra-zenith-agent's handler).
     */
    private void onCommandDelivery(String consumerTag, Delivery delivery) {
        String raw = new String(delivery.getBody(), StandardCharsets.UTF_8);
        try {
            JsonObject msg  = JsonParser.parseString(raw).getAsJsonObject();
            String type     = msg.has("type") ? msg.get("type").getAsString() : "";

            if ("PEARL_INVENTORY".equals(type)) {
                handlePearlInventory(msg);
                return;
            }

            if ("PEARL_SYNC_LEDGER".equals(type)) {
                handlePearlSyncLedger(msg);
                return;
            }

            if ("PEARL_CLEAR".equals(type)) {
                handlePearlClear(msg);
                return;
            }

            if ("PEARL_PURGE_PLAYERS".equals(type)) {
                handlePearlPurgePlayers(msg);
                return;
            }

            if (!"PEARL_LOAD".equals(type)) return; // not our command

            String commandId  = msg.has("commandId") ? msg.get("commandId").getAsString() : "";
            JsonObject data   = msg.has("data") ? msg.getAsJsonObject("data") : new JsonObject();

            String uuidStr    = data.has("playerUUID") ? data.get("playerUUID").getAsString() : null;
            String playerName = data.has("playerName") ? data.get("playerName").getAsString() : null;

            if (uuidStr == null || uuidStr.isBlank()) {
                publishResult(commandId, "error", -1, null, playerName);
                return;
            }

            UUID playerUUID;
            try {
                playerUUID = UUID.fromString(uuidStr);
            } catch (IllegalArgumentException ex) {
                publishResult(commandId, "error", -1, null, playerName);
                return;
            }

            // Hand off to the game tick handler — PearlManager methods must run on game thread.
            pendingLoads.add(new PearlLoadRequest(commandId, playerUUID, playerName));
        } catch (Exception e) {
            LOG.warn("[Hydra] Failed to parse PEARL_LOAD command: {}", e.getMessage());
        }
    }

    // ── Game tick handler (ZenithProxy game thread) ─────────────────────────────

    private void onGameTick(ClientBotTick event) {
        if (!hydraActive) return;

        // Process ledger updates first — purge must complete before any loads.
        List<HydraLedger.LedgerEntry> ledgerUpdate;
        while ((ledgerUpdate = pendingLedgerUpdates.poll()) != null) {
            processLedgerUpdate(ledgerUpdate);
        }

        PearlLoadRequest req;
        while ((req = pendingLoads.poll()) != null) {
            processPearlLoad(req);
        }

        // Pearl audit: run on first tick after connect, then every 5 minutes.
        if (auditOnNextTick) {
            auditOnNextTick = false;
            auditTickCounter = 0;
            // Delay audit by a few seconds to let entity cache populate.
            auditTickCounter = AUDIT_INTERVAL_TICKS - 100; // ~5 seconds from now
        }
        auditTickCounter++;
        if (auditTickCounter >= AUDIT_INTERVAL_TICKS) {
            auditTickCounter = 0;
            runPearlAudit();
        }
    }

    /**
     * Applies a ledger update: replaces the authorized user list, then purges
     * any stored pearl data for players who are no longer authorized.
     * Runs on the game thread — safe to modify PLUGIN_CONFIG.
     */
    private void processLedgerUpdate(List<HydraLedger.LedgerEntry> entries) {
        PearlPlusPlugin.LEDGER.update(entries);
        // Purge pearl data for any player not in the new ledger.
        int purged = pearlManager.purgeUnauthorized(PearlPlusPlugin.LEDGER.authorizedUUIDs());
        if (purged > 0) {
            LOG.info("[Hydra] Purged pearl data for {} unauthorized player(s) after ledger update", purged);
        }
    }

    /**
     * Resolves and triggers the pearl load for the given player UUID.
     * Runs on the game thread — safe to call PearlManager and Baritone.
     */
    private boolean isPlayerOnline(UUID playerUUID) {
        if (playerUUID == null) return false;
        return CACHE.getTabListCache().get(playerUUID).isPresent();
    }

    /**
     * Returns true if it's safe to process a PEARL_LOAD now. False = caller
     * should re-queue and wait another tick. Gated on:
     *   1. Bot has been online at least PEARL_LOAD_MIN_UPTIME_TICKS — covers
     *      the post-JoinGame settle window
     *   2. The chunk containing the target pearl is loaded — covers the case
     *      where the bot logged in far from the pearl base and chunks are
     *      still streaming in
     */
    private boolean readyForLoad(PearlPlusConfig.StoredPearl pearl) {
        if (onlineSinceMillis < 0) return false;
        long elapsedMs = System.currentTimeMillis() - onlineSinceMillis;
        if (elapsedMs < (PEARL_LOAD_MIN_UPTIME_TICKS * 50L)) return false;
        if (pearl == null || CACHE == null || CACHE.getChunkCache() == null) return false;
        Chunk chunk = CACHE.getChunkCache().get(pearl.x >> 4, pearl.z >> 4);
        return chunk != null;
    }

    private void processPearlLoad(PearlLoadRequest req) {
        // Authorization: reject loads from non-Hydra users.
        if (!PearlPlusPlugin.LEDGER.isAuthorized(req.playerUUID())) {
            LOG.info("[Hydra] Rejected PEARL_LOAD for unauthorized player {} ({})",
                req.playerName(), req.playerUUID());
            publishResult(req.commandId(), "error", -1, null, req.playerName());
            return;
        }

        // Don't waste pearls on offline players — check the server tab list first.
        if (!isPlayerOnline(req.playerUUID())) {
            publishResult(req.commandId(), "player_offline", -1, null, req.playerName());
            return;
        }

        PearlPlusConfig.PlayerPearls playerEntry = PLUGIN_CONFIG.players.get(req.playerUUID());

        if (playerEntry == null || playerEntry.pearls.isEmpty()) {
            publishResult(req.commandId(), "no_pearls", 0, null, req.playerName());
            return;
        }

        // Resolve which pearl to load — honours default and autoDefaultToPresent setting.
        String pearlId = pearlManager.defaultPearlId(req.playerUUID());
        if (pearlId == null) {
            publishResult(req.commandId(), "not_found", 0, null, req.playerName());
            return;
        }

        PearlPlusConfig.StoredPearl pearl = playerEntry.pearls.get(pearlId);
        if (pearl == null) {
            publishResult(req.commandId(), "not_found", 0, pearlId, req.playerName());
            return;
        }

        // Readiness gate: defer if we just entered the game and the pearl's
        // chunk isn't loaded yet. Up to PEARL_LOAD_MAX_DEFER_TICKS retries
        // (~3s), then we proceed anyway and let the downstream code fail
        // loudly. See c2 LazyPearlManager.lazyOnlineSettle for the matching
        // server-side settle window.
        if (req.deferredTicks() < PEARL_LOAD_MAX_DEFER_TICKS && !readyForLoad(pearl)) {
            pendingLoads.add(req.defer());
            return;
        }

        // Count present pearls BEFORE the load so we can report an accurate post-load count.
        // One pearl will be consumed by the load, so subtract one (floor at 0).
        int presentNow     = pearlManager.countPresentPearls(req.playerUUID());
        int postLoadCount  = Math.max(0, presentNow - 1);

        // Immediately publish "loading" — the Discord side has a 10-12 s timeout and the
        // Baritone path can take several seconds. Sending "loading" first keeps the interaction
        // alive and lets the user know the request was accepted.
        publishResult(req.commandId(), "loading", postLoadCount, pearlId, req.playerName());

        // Trigger the actual load (Baritone pathfinding → trapdoor right-click).
        String displayName = req.playerName() != null ? req.playerName() : req.playerUUID().toString();
        pearlManager.loadPearl(pearl, displayName);
    }

    // ── Result publisher ────────────────────────────────────────────────────────

    /**
     * Publishes an {@code agent.pearl.load} event to {@code hydra.events}.
     *
     * <p>The JSON envelope matches the format produced by hydra-zenith-agent's
     * {@code RabbitMQManager.doPublish()} so the Go consumer can parse it with the same
     * {@code zenithEnvelope} struct used for all other agent events.
     *
     * <p>The routing key {@code agent.<id>.agent.pearl.load} follows the same pattern as all
     * other agent events (e.g. {@code agent.<id>.agent.status}, {@code agent.<id>.proxy.online})
     * so the Go side's wildcard binding {@code agent.<id>.#} captures it automatically.
     */
    private void publishResult(String commandId, String status, int pearlCount,
                               String pearlId, String playerName) {
        if (!hydraActive || publishCh == null) return;

        // Inner data object
        JsonObject data = new JsonObject();
        data.addProperty("commandId",  commandId  != null ? commandId  : "");
        data.addProperty("status",     status);
        data.addProperty("pearlCount", pearlCount);
        if (pearlId    != null) data.addProperty("pearlId",    pearlId);
        if (playerName != null) data.addProperty("playerName", playerName);

        // Outer envelope — matches RabbitMQManager.doPublish() format
        JsonObject envelope = new JsonObject();
        envelope.addProperty("agentId",   agentId);
        envelope.addProperty("eventType", "agent.pearl.load");
        envelope.addProperty("ts",        System.currentTimeMillis());
        envelope.add("data", data);

        String routingKey = "agent." + agentId + ".agent.pearl.load";
        byte[] body = envelope.toString().getBytes(StandardCharsets.UTF_8);

        try {
            publishCh.basicPublish(
                EXCHANGE_EVENTS, routingKey,
                false, false,
                new AMQP.BasicProperties.Builder()
                    .contentType("application/json")
                    .deliveryMode(1) // non-persistent — transient result
                    .build(),
                body
            );
        } catch (IOException e) {
            LOG.warn("[Hydra] Failed to publish agent.pearl.load result: {}", e.getMessage());
        }
    }

    // ── PEARL_INVENTORY handler (runs on RabbitMQ consumer thread — read-only) ─

    /**
     * Handles {@code PEARL_INVENTORY} commands by reading all players from
     * {@code PLUGIN_CONFIG.players} and publishing an {@code agent.pearl.inventory}
     * event with the full pearl roster.
     *
     * <p>This is safe to run on the consumer thread because it only reads the
     * config map — no Baritone or entity-cache interaction required.
     */
    private void handlePearlInventory(JsonObject msg) {
        String commandId = msg.has("commandId") ? msg.get("commandId").getAsString() : "";

        JsonObject data = new JsonObject();
        data.addProperty("commandId", commandId);

        com.google.gson.JsonArray playersArr = new com.google.gson.JsonArray();
        for (var entry : PLUGIN_CONFIG.players.entrySet()) {
            UUID uuid = entry.getKey();
            PearlPlusConfig.PlayerPearls pp = entry.getValue();

            JsonObject playerObj = new JsonObject();
            playerObj.addProperty("uuid", uuid.toString());
            playerObj.addProperty("name", pp.playerName != null ? pp.playerName : "");
            playerObj.addProperty("pearlCount", pp.pearls.size());

            com.google.gson.JsonArray pearlIds = new com.google.gson.JsonArray();
            for (String id : pp.pearls.keySet()) {
                pearlIds.add(id);
            }
            playerObj.add("pearlIds", pearlIds);

            playersArr.add(playerObj);
        }
        data.add("players", playersArr);

        // Publish agent.pearl.inventory event
        JsonObject envelope = new JsonObject();
        envelope.addProperty("agentId", agentId);
        envelope.addProperty("eventType", "agent.pearl.inventory");
        envelope.addProperty("ts", System.currentTimeMillis());
        envelope.add("data", data);

        String routingKey = "agent." + agentId + ".agent.pearl.inventory";
        byte[] body = envelope.toString().getBytes(StandardCharsets.UTF_8);

        try {
            publishCh.basicPublish(
                EXCHANGE_EVENTS, routingKey,
                false, false,
                new AMQP.BasicProperties.Builder()
                    .contentType("application/json")
                    .deliveryMode(1)
                    .build(),
                body
            );
        } catch (IOException e) {
            LOG.warn("[Hydra] Failed to publish agent.pearl.inventory: {}", e.getMessage());
        }
    }

    // ── PEARL_SYNC_LEDGER handler (runs on RabbitMQ consumer thread — parse only) ─

    /**
     * Parses a {@code PEARL_SYNC_LEDGER} command and enqueues the entries for
     * processing on the game tick thread (where PLUGIN_CONFIG can be safely modified).
     *
     * <p>Inbound payload:
     * <pre>{@code
     * {
     *   "type": "PEARL_SYNC_LEDGER",
     *   "commandId": "<uuid>",
     *   "data": {
     *     "users": [
     *       {"uuid": "<mc-uuid>", "name": "<mc-name>", "rank": "intern"},
     *       {"uuid": "<mc-uuid>", "name": "<mc-name>", "rank": "guest"}
     *     ]
     *   }
     * }
     * }</pre>
     */
    private void handlePearlSyncLedger(JsonObject msg) {
        JsonObject data = msg.has("data") ? msg.getAsJsonObject("data") : new JsonObject();
        com.google.gson.JsonArray usersArr = data.has("users") ? data.getAsJsonArray("users") : new com.google.gson.JsonArray();

        List<HydraLedger.LedgerEntry> entries = new ArrayList<>();
        for (var element : usersArr) {
            if (!element.isJsonObject()) continue;
            JsonObject userObj = element.getAsJsonObject();

            String uuidStr = userObj.has("uuid") ? userObj.get("uuid").getAsString() : null;
            String name = userObj.has("name") ? userObj.get("name").getAsString() : "";
            String rank = userObj.has("rank") ? userObj.get("rank").getAsString() : "";

            if (uuidStr == null || uuidStr.isBlank()) continue;
            try {
                UUID uuid = UUID.fromString(uuidStr);
                entries.add(new HydraLedger.LedgerEntry(uuid, name, rank));
            } catch (IllegalArgumentException e) {
                LOG.warn("[Hydra] Skipping invalid UUID in ledger: {}", uuidStr);
            }
        }

        LOG.info("[Hydra] Received PEARL_SYNC_LEDGER with {} user(s), queuing for game thread", entries.size());
        pendingLedgerUpdates.add(entries);
    }

    // ── External pearl pop notification ────────────────────────────────────────

    /**
     * Publishes an {@code agent.pearl.popped} event when a pearl disappears
     * without the bot loading it. Called by AutoDetectModule.
     */
    public void publishExternalPearlPop(String pearlId, String ownerSummary, int x, int y, int z) {
        if (!hydraActive || publishCh == null) return;

        JsonObject data = new JsonObject();
        data.addProperty("pearlId", pearlId != null ? pearlId : "unknown");
        data.addProperty("owner", ownerSummary);
        data.addProperty("x", x);
        data.addProperty("y", y);
        data.addProperty("z", z);
        data.addProperty("botLoaded", false);

        JsonObject envelope = new JsonObject();
        envelope.addProperty("agentId", agentId);
        envelope.addProperty("eventType", "agent.pearl.popped");
        envelope.addProperty("ts", System.currentTimeMillis());
        envelope.add("data", data);

        String routingKey = "agent." + agentId + ".agent.pearl.popped";
        byte[] body = envelope.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8);

        try {
            publishCh.basicPublish(
                EXCHANGE_EVENTS, routingKey,
                false, false,
                new AMQP.BasicProperties.Builder()
                    .contentType("application/json")
                    .deliveryMode(1)
                    .build(),
                body
            );
        } catch (IOException e) {
            LOG.warn("[Hydra] Failed to publish pearl.popped: {}", e.getMessage());
        }
    }

    // ── Pearl audit ──────────────────────────────────────────────────────────────

    /**
     * Scans all stored pearl records against live entities and publishes an
     * {@code agent.pearl.audit} event reporting total expected, total found,
     * and any missing pearl IDs.
     * Runs on the game thread — safe to read entity cache and PLUGIN_CONFIG.
     */
    private void runPearlAudit() {
        if (PLUGIN_CONFIG.players.isEmpty()) return;

        int totalExpected = 0;
        int totalPresent = 0;
        com.google.gson.JsonArray missingArr = new com.google.gson.JsonArray();
        com.google.gson.JsonArray playersArr = new com.google.gson.JsonArray();

        for (var entry : PLUGIN_CONFIG.players.entrySet()) {
            UUID uuid = entry.getKey();
            PearlPlusConfig.PlayerPearls pp = entry.getValue();
            if (pp == null || pp.pearls == null) continue;

            int playerExpected = pp.pearls.size();
            int playerPresent = 0;

            for (var pearlEntry : pp.pearls.entrySet()) {
                PearlPlusConfig.StoredPearl pearl = pearlEntry.getValue();
                totalExpected++;
                if (pearlManager.isPearlPresent(pearl)) {
                    totalPresent++;
                    playerPresent++;
                } else {
                    JsonObject missing = new JsonObject();
                    missing.addProperty("pearlId", pearl.pearlId);
                    missing.addProperty("owner", pp.playerName != null ? pp.playerName : uuid.toString());
                    missing.addProperty("x", pearl.x);
                    missing.addProperty("y", pearl.y);
                    missing.addProperty("z", pearl.z);
                    missingArr.add(missing);
                }
            }

            JsonObject playerObj = new JsonObject();
            playerObj.addProperty("uuid", uuid.toString());
            playerObj.addProperty("name", pp.playerName != null ? pp.playerName : "");
            playerObj.addProperty("expected", playerExpected);
            playerObj.addProperty("present", playerPresent);
            playersArr.add(playerObj);
        }

        // Publish audit result.
        JsonObject data = new JsonObject();
        data.addProperty("totalExpected", totalExpected);
        data.addProperty("totalPresent", totalPresent);
        data.addProperty("totalMissing", totalExpected - totalPresent);
        data.add("missing", missingArr);
        data.add("players", playersArr);

        JsonObject envelope = new JsonObject();
        envelope.addProperty("agentId", agentId);
        envelope.addProperty("eventType", "agent.pearl.audit");
        envelope.addProperty("ts", System.currentTimeMillis());
        envelope.add("data", data);

        String routingKey = "agent." + agentId + ".agent.pearl.audit";
        byte[] body = envelope.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8);

        try {
            publishCh.basicPublish(
                EXCHANGE_EVENTS, routingKey,
                false, false,
                new AMQP.BasicProperties.Builder()
                    .contentType("application/json")
                    .deliveryMode(1)
                    .build(),
                body
            );
            if (totalExpected > totalPresent) {
                LOG.info("[Hydra] Pearl audit: {}/{} present, {} missing", totalPresent, totalExpected, totalExpected - totalPresent);
            }
        } catch (IOException e) {
            LOG.warn("[Hydra] Failed to publish pearl audit: {}", e.getMessage());
        }
    }

    // ── PEARL_CLEAR handler ──────────────────────────────────────────────────────

    /**
     * Handles {@code PEARL_CLEAR} commands by removing all stored pearl data.
     * Sent by the C2 when an agent is unassigned from a base or the base is
     * removed — prevents the bot from pathfinding millions of blocks to old
     * pearl locations.
     *
     * <p>Publishes an {@code agent.pearl.clear} acknowledgement event.
     */
    private void handlePearlClear(JsonObject msg) {
        String commandId = msg.has("commandId") ? msg.get("commandId").getAsString() : "";

        int count = PLUGIN_CONFIG.players.size();
        PLUGIN_CONFIG.players.clear();
        if (PearlPlusPlugin.AUTO_DETECT != null) {
            PearlPlusPlugin.AUTO_DETECT.resetTracking();
        }
        LOG.info("[Hydra] PEARL_CLEAR: removed all pearl data ({} player entries)", count);

        // Publish acknowledgement
        JsonObject data = new JsonObject();
        data.addProperty("commandId", commandId);
        data.addProperty("status", "cleared");
        data.addProperty("playersCleared", count);

        JsonObject envelope = new JsonObject();
        envelope.addProperty("agentId", agentId);
        envelope.addProperty("eventType", "agent.pearl.clear");
        envelope.addProperty("ts", System.currentTimeMillis());
        envelope.add("data", data);

        String routingKey = "agent." + agentId + ".agent.pearl.clear";
        byte[] body = envelope.toString().getBytes(StandardCharsets.UTF_8);

        try {
            publishCh.basicPublish(
                EXCHANGE_EVENTS, routingKey,
                false, false,
                new AMQP.BasicProperties.Builder()
                    .contentType("application/json")
                    .deliveryMode(1)
                    .build(),
                body
            );
        } catch (IOException e) {
            LOG.warn("[Hydra] Failed to publish agent.pearl.clear: {}", e.getMessage());
        }
    }

    // ── PEARL_PURGE_PLAYERS handler ───────────────────────────────────────────

    /**
     * Handles {@code PEARL_PURGE_PLAYERS} commands by removing pearl data for
     * specific player UUIDs. Sent by the C2 when users are purged from Hydra
     * (left the guild, lost all roles, etc.).
     *
     * <p>Inbound payload:
     * <pre>{@code
     * {
     *   "type": "PEARL_PURGE_PLAYERS",
     *   "commandId": "<uuid>",
     *   "data": {
     *     "uuids": ["<mc-uuid>", "<mc-uuid>", ...]
     *   }
     * }
     * }</pre>
     */
    private void handlePearlPurgePlayers(JsonObject msg) {
        String commandId = msg.has("commandId") ? msg.get("commandId").getAsString() : "";
        JsonObject data = msg.has("data") ? msg.getAsJsonObject("data") : new JsonObject();
        JsonArray uuids = data.has("uuids") ? data.getAsJsonArray("uuids") : new JsonArray();

        int removed = 0;
        for (var elem : uuids) {
            try {
                UUID uuid = UUID.fromString(elem.getAsString());
                if (PLUGIN_CONFIG.players.remove(uuid) != null) {
                    removed++;
                }
            } catch (Exception ignored) {}
        }

        if (PearlPlusPlugin.AUTO_DETECT != null && removed > 0) {
            PearlPlusPlugin.AUTO_DETECT.resetTracking();
        }
        LOG.info("[Hydra] PEARL_PURGE_PLAYERS: removed {}/{} player entries", removed, uuids.size());

        // Publish acknowledgement
        JsonObject ack = new JsonObject();
        ack.addProperty("commandId", commandId);
        ack.addProperty("status", "purged");
        ack.addProperty("playersRemoved", removed);

        JsonObject envelope = new JsonObject();
        envelope.addProperty("agentId", agentId);
        envelope.addProperty("eventType", "agent.pearl.purge");
        envelope.addProperty("ts", System.currentTimeMillis());
        envelope.add("data", ack);

        String routingKey = "agent." + agentId + ".agent.pearl.purge";
        byte[] body = envelope.toString().getBytes(StandardCharsets.UTF_8);

        try {
            publishCh.basicPublish(
                EXCHANGE_EVENTS, routingKey,
                false, false,
                new AMQP.BasicProperties.Builder()
                    .contentType("application/json")
                    .deliveryMode(1)
                    .build(),
                body
            );
        } catch (IOException e) {
            LOG.warn("[Hydra] Failed to publish agent.pearl.purge: {}", e.getMessage());
        }
    }

    // ── Internal types ──────────────────────────────────────────────────────────

    private record PearlLoadRequest(String commandId, UUID playerUUID, String playerName, int deferredTicks) {
        PearlLoadRequest(String commandId, UUID playerUUID, String playerName) {
            this(commandId, playerUUID, playerName, 0);
        }
        PearlLoadRequest defer() {
            return new PearlLoadRequest(commandId, playerUUID, playerName, deferredTicks + 1);
        }
    }
}
