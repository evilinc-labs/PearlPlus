package dev.zenith.pearlplus.hydra;

import dev.zenith.pearlplus.PearlPlusConfig;
import dev.zenith.pearlplus.PearlPlusPlugin;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

import static com.zenith.Globals.saveConfig;

// Owns the sidecar presence config. Reads PLUGIN_CONFIG.players read-only and
// only ever writes its own file — registration is never touched here. All
// access is synchronized: observe() runs on the game thread, count reads can
// come from the RabbitMQ consumer thread.
public final class PearlStateStore {
    private static final String CONFIG_FILE = "pearlplus-state";

    private final PearlStateConfig config;

    public PearlStateStore() {
        this.config = PearlPlusPlugin.API.registerConfig(CONFIG_FILE, PearlStateConfig.class);
    }

    // Pure decision. observable = bot ready + chunk loaded + in range. When we
    // cannot see the spot, the answer is UNKNOWN, never POPPED. Only a positive
    // in-range look flips PRESENT <-> POPPED.
    public static PearlPresence evaluate(boolean observable, boolean entityPresent) {
        if (!observable) return PearlPresence.UNKNOWN;
        return entityPresent ? PearlPresence.PRESENT : PearlPresence.POPPED;
    }

    // Record an observation. Returns the PRIOR state so callers can detect a
    // transition (e.g. to publish a state event). UNKNOWN is a no-op: we keep
    // the last known state so it survives the bot moving away / relogging.
    public synchronized PearlPresence observe(UUID owner, String pearlId, PearlPresence state, long nowMillis) {
        PearlPresence prior = stateOf(owner, pearlId);
        if (state == PearlPresence.UNKNOWN) return prior;
        Map<String, PearlStateConfig.Observation> byPearl =
            config.observations.computeIfAbsent(owner, k -> new LinkedHashMap<>());
        PearlStateConfig.Observation obs =
            byPearl.computeIfAbsent(pearlId, k -> new PearlStateConfig.Observation());
        obs.state = state.name();
        obs.lastObservedMillis = nowMillis;
        // Flush to disk immediately so state survives a kill/restart.
        // Runs on the game tick thread — same thread ZenithProxy uses for its own saves.
        saveConfig();
        return prior;
    }

    public synchronized PearlPresence stateOf(UUID owner, String pearlId) {
        Map<String, PearlStateConfig.Observation> byPearl = config.observations.get(owner);
        if (byPearl == null) return PearlPresence.UNKNOWN;
        PearlStateConfig.Observation obs = byPearl.get(pearlId);
        return obs == null ? PearlPresence.UNKNOWN : PearlPresence.fromString(obs.state);
    }

    public synchronized long lastObservedMillis(UUID owner, String pearlId) {
        Map<String, PearlStateConfig.Observation> byPearl = config.observations.get(owner);
        if (byPearl == null) return 0L;
        PearlStateConfig.Observation obs = byPearl.get(pearlId);
        return obs == null ? 0L : obs.lastObservedMillis;
    }

    // Drop sidecar rows whose registration no longer exists, so the sidecar
    // can't outlive an admin removal. Reads PLUGIN_CONFIG.players read-only and
    // only prunes our own map.
    public synchronized void pruneToLedger() {
        Map<UUID, PearlPlusConfig.PlayerPearls> players = PearlPlusPlugin.PLUGIN_CONFIG.players;
        config.observations.entrySet().removeIf(entry -> {
            PearlPlusConfig.PlayerPearls pp = players.get(entry.getKey());
            if (pp == null || pp.pearls == null) return true;
            entry.getValue().keySet().removeIf(pearlId -> !pp.pearls.containsKey(pearlId));
            return entry.getValue().isEmpty();
        });
    }
}
