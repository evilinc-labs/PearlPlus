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

    // secret/hydraUser are optional: secret accounts carry a codename as `name`
    // (never the real username) and the owning Hydra user's alias as `hydraUser`,
    // so the bot can render "*codename* [hydraUser]" and treat the codename as
    // authoritative everywhere it would otherwise surface the real name.
    public record LedgerEntry(UUID uuid, String name, String rank, boolean secret, String hydraUser) {}

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
     * Check if a Minecraft UUID is authorized at this base.
     *
     * <p>When Hydra integration is active (ledger has been received), only
     * UUIDs present in the ledger are authorized. When running standalone
     * (no C2 / no ledger ever received), returns {@code true} for all
     * non-null UUIDs to preserve backwards-compatible open-registration.
     */
    public boolean isAuthorized(UUID uuid) {
        if (uuid == null) return false;
        if (!ledgerReceived) return true; // standalone mode — no C2
        return authorized.containsKey(uuid);
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

    /** Ledger entry for a UUID, or null. */
    public LedgerEntry get(UUID uuid) {
        return uuid == null ? null : authorized.get(uuid);
    }

    /** True if the UUID belongs to a secret (codename-only) account. */
    public boolean isSecret(UUID uuid) {
        LedgerEntry e = get(uuid);
        return e != null && e.secret();
    }

    /**
     * Safe display label for a player. For a secret account this is the italic
     * codename followed by [hydraUser] — never the real username; for everyone
     * else it is the provided observed name unchanged. mcFormat=true uses
     * Minecraft format codes (§o italic / §r reset) for in-game chat; false uses
     * markdown-style asterisks for logs and Discord.
     */
    public String displayLabel(UUID uuid, String observedName, boolean mcFormat) {
        LedgerEntry e = get(uuid);
        if (e == null || !e.secret()) {
            return observedName;
        }
        String hu = (e.hydraUser() != null && !e.hydraUser().isBlank()) ? e.hydraUser() : "?";
        if (mcFormat) {
            return "§o" + e.name() + "§r [" + hu + "]";
        }
        return "*" + e.name() + "* [" + hu + "]";
    }

    /**
     * The name to STORE for a player's pearls. For secret accounts this is the
     * codename (so the real username never persists to the bot's pearl config or
     * downstream reads); for everyone else it is the observed name unchanged.
     */
    public String storedName(UUID uuid, String observedName) {
        LedgerEntry e = get(uuid);
        if (e != null && e.secret()) {
            return e.name(); // codename
        }
        return observedName;
    }
}
