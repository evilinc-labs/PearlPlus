package dev.zenith.pearlplus.hydra;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

// Sidecar presence-state for registered pearls. Persisted SEPARATELY from the
// pearl ledger at plugins/config/pearlplus-state.json. This is fork-owned
// observed state only — it never mirrors or mutates PLUGIN_CONFIG.players
// (the admin-owned registration). Keyed by the same (ownerUuid, pearlId) so it
// joins to the ledger without owning it. Registration stays the source of truth
// for which pearls exist; this only records whether we last saw them.
public class PearlStateConfig {
    // ownerUuid -> (pearlId -> observation)
    public final Map<UUID, Map<String, Observation>> observations = new LinkedHashMap<>();

    public static final class Observation {
        // PRESENT | POPPED | UNKNOWN — see PearlPresence
        public String state = "UNKNOWN";
        // epoch millis of the last positive (in-range) observation; 0 = never observed
        public long lastObservedMillis = 0L;
    }
}
