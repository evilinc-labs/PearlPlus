package dev.zenith.pearlplus.hydra;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static dev.zenith.pearlplus.PearlPlusPlugin.LOG;

/**
 * Thread-safe in-memory store of Hydra-authorized Minecraft UUIDs.
 *
 * <p>Populated by {@code PEARL_SYNC_LEDGER} commands pushed from the C2 on
 * agent startup and periodically thereafter. Checked by {@code AutoDetectModule}
 * before registering any pearl — pearls thrown by users not in the ledger are
 * silently ignored.
 *
 * <p>In standalone deployments (no Hydra integration) the ledger is never
 * populated and {@link #isAuthorized} returns {@code true} for all UUIDs,
 * preserving backwards-compatible open-registration behaviour.
 */
public class HydraLedger {

    public record LedgerEntry(UUID uuid, String name, String rank) {}

    private volatile Map<UUID, LedgerEntry> authorized = new ConcurrentHashMap<>();
    private volatile boolean ledgerReceived = false;

    /**
     * Replace the entire authorized users list.
     * Called on the game tick thread after a {@code PEARL_SYNC_LEDGER} command
     * is dequeued from the pending queue.
     */
    public void update(List<LedgerEntry> entries) {
        Map<UUID, LedgerEntry> newMap = new ConcurrentHashMap<>();
        for (LedgerEntry entry : entries) {
            newMap.put(entry.uuid(), entry);
        }
        this.authorized = newMap;
        this.ledgerReceived = true;
        LOG.info("[Hydra Ledger] Updated: {} authorized user(s)", newMap.size());
    }

    /**
     * Check if a Minecraft UUID is authorized to place pearls at this base.
     *
     * <p>Currently returns {@code true} for all non-null UUIDs — ledger
     * enforcement is disabled so all players' pearls are respected.
     */
    public boolean isAuthorized(UUID uuid) {
        return uuid != null;
    }

    /**
     * Returns {@code true} once the first ledger has been received from the C2.
     */
    public boolean hasLedger() {
        return ledgerReceived;
    }

    /**
     * Returns a snapshot of all currently authorized UUIDs.
     * Used by the purge routine to remove pearls from deauthorized players.
     */
    public Set<UUID> authorizedUUIDs() {
        return new HashSet<>(authorized.keySet());
    }
}
