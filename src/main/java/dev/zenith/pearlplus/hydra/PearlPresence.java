package dev.zenith.pearlplus.hydra;

// Observed presence of a registered pearl. UNKNOWN is deliberately distinct
// from POPPED: out of range, chunk not loaded, or bot offline is UNKNOWN —
// never POPPED. We only conclude a pearl is gone when we positively looked.
public enum PearlPresence {
    PRESENT,
    POPPED,
    UNKNOWN;

    public static PearlPresence fromString(String s) {
        if (s == null) return UNKNOWN;
        try {
            return valueOf(s);
        } catch (IllegalArgumentException e) {
            return UNKNOWN;
        }
    }
}
