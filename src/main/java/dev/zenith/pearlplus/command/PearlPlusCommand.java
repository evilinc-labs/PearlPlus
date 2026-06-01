package dev.zenith.pearlplus.command;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.mojang.brigadier.builder.LiteralArgumentBuilder;
import com.zenith.command.api.Command;
import com.zenith.command.api.CommandCategory;
import com.zenith.command.api.CommandContext;
import com.zenith.command.api.CommandUsage;
import com.zenith.discord.Embed;
import com.zenith.feature.api.minetools.MinetoolsApi;
import com.zenith.feature.api.minetools.model.MinetoolsUuidResponse;
import dev.zenith.pearlplus.PearlPlusPlugin;
import dev.zenith.pearlplus.module.AutoLoadModule;
import dev.zenith.pearlplus.module.AutoDetectModule;
import dev.zenith.pearlplus.module.PearlManager;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import static com.mojang.brigadier.arguments.IntegerArgumentType.getInteger;
import static com.mojang.brigadier.arguments.IntegerArgumentType.integer;
import static com.zenith.Globals.MODULE;
import static com.zenith.command.brigadier.CustomStringArgumentType.getString;
import static com.zenith.command.brigadier.CustomStringArgumentType.wordWithChars;
import static com.zenith.command.brigadier.ToggleArgumentType.getToggle;
import static com.zenith.command.brigadier.ToggleArgumentType.toggle;
import static dev.zenith.pearlplus.PearlPlusPlugin.PLUGIN_CONFIG;
import static dev.zenith.pearlplus.PearlPlusPlugin.LOG;

public class PearlPlusCommand extends Command {

    // Minetools (api.minetools.eu) has been intermittently dead — 503s,
    // timeouts. Fall back to Mojang's official lookup when it fails so
    // `pp add <user>` etc. don't bail with "Invalid username" for real names.
    //
    // Mojang's limit is ~60 req/min/IP with a sliding window. We cap ourselves
    // well under that and cache aggressively so we never hammer them:
    //   - min interval between Mojang calls: 1500ms (~40 req/min)
    //   - positive cache: 1 hour (usernames are stable; new owners get the
    //     name only after 37-day reservation, so 1h staleness is fine)
    //   - negative cache: 5 minutes (so a transient outage doesn't cause us
    //     to retry-spam, but real fixes propagate within 5 min)

    private static final HttpClient HTTP = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(4))
            .build();

    private static final long MOJANG_MIN_INTERVAL_MS = 1500L;
    private static final long POSITIVE_TTL_MS = 60L * 60L * 1000L;
    private static final long NEGATIVE_TTL_MS = 5L * 60L * 1000L;
    private static final long MAX_BACKOFF_WAIT_MS = 5000L;

    private record CacheEntry(Optional<UUID> uuid, long expiresAt) {}
    private static final Map<String, CacheEntry> UUID_CACHE = new ConcurrentHashMap<>();
    private static final AtomicLong lastMojangCallMillis = new AtomicLong(0L);

    private static Optional<UUID> resolveUuid(String name) {
        if (name == null || name.isBlank()) return Optional.empty();
        String key = name.toLowerCase(Locale.ROOT);

        CacheEntry cached = UUID_CACHE.get(key);
        long now = System.currentTimeMillis();
        if (cached != null && cached.expiresAt > now) return cached.uuid;

        // Try Minetools first — separate service, its own limits.
        try {
            Optional<MinetoolsUuidResponse> mt = MinetoolsApi.INSTANCE.getProfileFromUsername(name);
            if (mt.isPresent() && mt.get().uuid() != null) {
                Optional<UUID> r = Optional.of(mt.get().uuid());
                UUID_CACHE.put(key, new CacheEntry(r, now + POSITIVE_TTL_MS));
                return r;
            }
        } catch (Throwable ignored) {}

        // Fall back to Mojang with self-imposed rate limit.
        Optional<UUID> mojang = mojangLookup(name);
        long ttl = mojang.isPresent() ? POSITIVE_TTL_MS : NEGATIVE_TTL_MS;
        UUID_CACHE.put(key, new CacheEntry(mojang, now + ttl));
        return mojang;
    }

    // Resolve the player a management command targets. Existing registrations are
    // matched against the local ledger by stored name FIRST — that works even when
    // minetools/mojang are down or the player has since renamed. Names we don't
    // already know fall through to the external lookup (minetools -> mojang).
    private static Optional<UUID> resolveOwner(String name) {
        if (name == null || name.isBlank()) return Optional.empty();
        for (var e : PLUGIN_CONFIG.players.entrySet()) {
            var pp = e.getValue();
            if (pp != null && pp.playerName != null && pp.playerName.equalsIgnoreCase(name)) {
                return Optional.of(e.getKey());
            }
        }
        return resolveUuid(name);
    }

    private static Optional<UUID> mojangLookup(String name) {
        // Token-bucket-of-one: each call must be ≥ MOJANG_MIN_INTERVAL_MS
        // after the previous one. If we'd be too soon, sleep up to
        // MAX_BACKOFF_WAIT_MS; if even that wouldn't be enough, bail.
        while (true) {
            long now = System.currentTimeMillis();
            long last = lastMojangCallMillis.get();
            long earliest = last + MOJANG_MIN_INTERVAL_MS;
            if (now >= earliest) {
                if (lastMojangCallMillis.compareAndSet(last, now)) break;
                continue; // lost race, re-read
            }
            long wait = earliest - now;
            if (wait > MAX_BACKOFF_WAIT_MS) {
                LOG.warn("Mojang lookup for {} skipped — rate limit window {}ms exceeds max backoff", name, wait);
                return Optional.empty();
            }
            try { Thread.sleep(wait); } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return Optional.empty();
            }
        }

        try {
            HttpRequest req = HttpRequest.newBuilder()
                    .uri(URI.create("https://api.mojang.com/users/profiles/minecraft/" + name))
                    .timeout(Duration.ofSeconds(5))
                    .header("User-Agent", "PearlPlus")
                    .GET()
                    .build();
            HttpResponse<String> resp = HTTP.send(req, HttpResponse.BodyHandlers.ofString());
            if (resp.statusCode() == 429) {
                LOG.warn("Mojang rate-limited us on {} — cooling off", name);
                return Optional.empty();
            }
            if (resp.statusCode() != 200 || resp.body() == null || resp.body().isBlank()) {
                return Optional.empty();
            }
            JsonObject obj = JsonParser.parseString(resp.body()).getAsJsonObject();
            if (!obj.has("id")) return Optional.empty();
            String hex = obj.get("id").getAsString();
            if (hex.length() != 32) return Optional.empty();
            String dashed = hex.substring(0, 8) + "-" + hex.substring(8, 12) + "-"
                    + hex.substring(12, 16) + "-" + hex.substring(16, 20) + "-" + hex.substring(20);
            return Optional.of(UUID.fromString(dashed));
        } catch (Throwable t) {
            LOG.warn("Mojang lookup failed for {}: {}", name, t.getMessage());
            return Optional.empty();
        }
    }

    @Override
    public CommandUsage commandUsage() {
        return CommandUsage.builder()
            .name("pearlplus")
            .category(CommandCategory.MODULE)
            .description("Allow players to load pearls without whitelist through whispers.")
            .usageLines(
                "<on/off>",
                "list",
                "list clear",
                "add <playerName> <pearlId> <x> <y> <z>",
                "del <playerName> <pearlId>",
                "defaultpearlid <word|none>",
                "load <playerName> <pearlId>",
                "returnpos <on/off>",
                "strict <on/off>",
                "autodetect <on/off>",
                "autodetect temp <on/off>",
                "distancecheck <on/off>",
                "autodefault <on/off>",
                "whitelist <on/off / add / clear / list / remove>",
                "droppearlafterload <on/off>"
            )
            .aliases("pp")
            .build();
    }

    @Override
    public LiteralArgumentBuilder<CommandContext> register() {
        LiteralArgumentBuilder<CommandContext> builder = command("pearlplus")
                .requires(Command::validateAccountOwner);

        builder.then(argument("toggle", toggle()).executes(c -> {
            boolean enabled = getToggle(c, "toggle");
            PLUGIN_CONFIG.autoLoad.enabled = enabled;
            MODULE.get(AutoLoadModule.class).syncEnabledFromConfig();
            c.getSource().getEmbed()
                    .title("PearlPlus " + toggleStrCaps(enabled));
            return 0;
        }));

        builder.then(literal("list")
                .executes(c -> {
                    PearlManager manager = new PearlManager(MODULE.get(AutoDetectModule.class));
                    String pearls = manager.pearlsListWithCoordsAllPlayers();
                    c.getSource().getEmbed().title("All Pearls").description(pearls);
                    return 0;
                })
                .then(literal("clear").executes(c -> {
                    int playerCount = PLUGIN_CONFIG.players.size();
                    int pearlCount = PLUGIN_CONFIG.players.values().stream()
                            .mapToInt(playerPearls -> playerPearls.pearls.size())
                            .sum();
                    PLUGIN_CONFIG.players.clear();
                    if (PearlPlusPlugin.AUTO_DETECT != null) {
                        PearlPlusPlugin.AUTO_DETECT.resetTracking();
                    }
                    c.getSource().getEmbed()
                            .title("Cleared pearls (" + pearlCount + " pearls removed from " + playerCount + " players)");
                    LOG.info("Cleared pearls ({} pearls removed from {} players)", pearlCount, playerCount);
                    return 0;
                }))
                .then(argument("playerName", wordWithChars()).executes(c -> {
                    String name = getString(c, "playerName");
                    Optional<UUID> result = resolveOwner(name);
                    if (result.isEmpty()) {
                        c.getSource().getEmbed().title("Invalid username: " + name);
                        return 0;
                    }
                    UUID uuid = result.get();
                    PearlManager manager = new PearlManager(MODULE.get(AutoDetectModule.class));
                    String pearls = manager.pearlsListWithCoords(uuid);
                    c.getSource().getEmbed().title("Pearls for " + name).description(pearls);
                    return 0;
                })));

        builder.then(literal("add")
                .then(argument("playerName", wordWithChars())
                        .then(argument("pearlId", wordWithChars())
                                .then(argument("x", integer())
                                        .then(argument("y", integer())
                                                .then(argument("z", integer()).executes(c -> {
                                                    String name = getString(c, "playerName");
                                                    String pearlId = getString(c, "pearlId");
                                                    Optional<UUID> result = resolveOwner(name);
                                                    if (result.isEmpty()) {
                                                        c.getSource().getEmbed().title("Invalid username: " + name);
                                                        return 0;
                                                    }

                                                    int x = getInteger(c, "x");
                                                    int y = getInteger(c, "y");
                                                    int z = getInteger(c, "z");

                                                    UUID uuid = result.get();
                                                    PearlManager manager = new PearlManager(MODULE.get(AutoDetectModule.class));
                                                    manager.recordPearl(uuid, name, pearlId, x, y, z);
                                                    c.getSource().getEmbed()
                                                            .title("Pearl stored for " + name)
                                                            .description(String.format("%s at %d %d %d", pearlId, x, y, z));
                                                    return 0;
                                                })))))));

        builder.then(literal("del")
                .then(argument("playerName", wordWithChars())
                        .then(argument("pearlId", wordWithChars()).executes(c -> {
                            String name = getString(c, "playerName");
                            String pearlId = getString(c, "pearlId");
                            Optional<UUID> result = resolveOwner(name);
                            if (result.isEmpty()) {
                                c.getSource().getEmbed().title("Invalid username: " + name);
                                return 0;
                            }

                            UUID uuid = result.get();
                            PearlManager manager = new PearlManager(MODULE.get(AutoDetectModule.class));
                            String resolvedPearlId = manager.resolvePearlId(uuid, pearlId);
                            if (resolvedPearlId == null) {
                                c.getSource().getEmbed().title("Pearl not found for " + name);
                                return 0;
                            }

                            manager.removePearl(uuid, resolvedPearlId);
                            c.getSource().getEmbed().title("Removed pearl " + resolvedPearlId + " for " + name);
                            return 0;
                        }))));

        builder.then(literal("load")
                .then(argument("playerName", wordWithChars())
                        .then(argument("pearlId", wordWithChars()).executes(c -> {
                            String name = getString(c, "playerName");
                            String pearlId = getString(c, "pearlId");
                            Optional<UUID> result = resolveOwner(name);
                            if (result.isEmpty()) {
                                c.getSource().getEmbed().title("Invalid username: " + name);
                                return 0;
                            }

                            UUID uuid = result.get();
                            PearlManager manager = new PearlManager(MODULE.get(AutoDetectModule.class));
                            String resolvedPearlId = manager.resolvePearlId(uuid, pearlId);
                            if (resolvedPearlId == null) {
                                c.getSource().getEmbed().title("Pearl not found for " + name);
                                return 0;
                            }

                            var playerEntry = PLUGIN_CONFIG.players.get(uuid);
                            if (playerEntry == null || !playerEntry.pearls.containsKey(resolvedPearlId)) {
                                c.getSource().getEmbed().title("No authorized pearls found for " + name);
                                return 0;
                            }

                            manager.loadPearl(playerEntry.pearls.get(resolvedPearlId), null);
                            c.getSource().getEmbed().title("Loading pearl " + resolvedPearlId + " for " + name);
                            return 0;
                        }))));
        
        builder.then(literal("defaultpearlid")
                .then(argument("word", wordWithChars()).executes(c -> {
                    String word = getString(c, "word");
                    if ("none".equalsIgnoreCase(word)) {
                        PLUGIN_CONFIG.defaultPearlId = null;
                        c.getSource().getEmbed().title("Pearl ID word cleared; using player names");
                    } else {
                        PLUGIN_CONFIG.defaultPearlId = word;
                        c.getSource().getEmbed().title("Pearl ID word set to '" + word + "'");
                    }
                    return 0;
                })));

        builder.then(literal("strict")
                .then(argument("toggle", toggle()).executes(c -> {
                    boolean strict = getToggle(c, "toggle");
                    PLUGIN_CONFIG.autoLoad.allowNoiseAfterPearl = !strict;
                    c.getSource().getEmbed()
                            .title("PearlPlus strict " + toggleStrCaps(strict));
                    return 0;
                })));

        builder.then(literal("returnpos")
                .then(argument("toggle", toggle()).executes(c -> {
                    boolean enabled = getToggle(c, "toggle");
                    PLUGIN_CONFIG.autoLoad.returnToStartPos = enabled;
                    c.getSource().getEmbed()
                            .title("PearlPlus Return to Start " + toggleStrCaps(enabled));
                    return 0;
                })));

        builder.then(literal("autodetect")
                .then(argument("toggle", toggle()).executes(c -> {
                    boolean enabled = getToggle(c, "toggle");
                    PLUGIN_CONFIG.autoDetect.enabled = enabled;

                    AutoDetectModule module = MODULE.get(AutoDetectModule.class);
                    module.syncEnabledFromConfig();
                    if (enabled) {
                        module.markExistingPearls();
                    }

                    c.getSource().getEmbed()
                            .title("PearlPlus Autodetect " + toggleStrCaps(enabled));
                    return 0;
                }))
                .then(literal("temp")
                        .then(argument("toggle", toggle()).executes(c -> {
                            boolean enabled = getToggle(c, "toggle");
                            PLUGIN_CONFIG.autoDetect.temporaryMode = enabled;

                            AutoDetectModule module = MODULE.get(AutoDetectModule.class);
                            module.onTemporaryModeToggle(enabled);

                            c.getSource().getEmbed()
                                    .title("PearlPlus Autodetect Temp Mode " + toggleStrCaps(enabled));
                            return 0;
                        }))));

        builder.then(literal("distancecheck")
                .then(argument("toggle", toggle()).executes(c -> {
                    boolean enabled = getToggle(c, "toggle");
                    PLUGIN_CONFIG.autoDetect.distanceCheck = enabled;

                    c.getSource().getEmbed()
                            .title("PearlPlus Distance Check " + toggleStrCaps(enabled));
                    return 0;
                })));

        builder.then(literal("autodefault")
                .then(argument("toggle", toggle()).executes(c -> {
                    boolean enabled = getToggle(c, "toggle");
                    PLUGIN_CONFIG.autoLoad.autoDefaultToPresent = enabled;
                    c.getSource().getEmbed()
                            .title("PearlPlus Auto Default " + toggleStrCaps(enabled));
                    return 0;
                })));

        builder.then(literal("whitelist")
                .then(argument("toggle", toggle()).executes(c -> {
                    boolean enabled = getToggle(c, "toggle");
                    PLUGIN_CONFIG.autoLoad.whitelistEnabled = enabled;
                    c.getSource().getEmbed()
                            .title("Whitelist " + toggleStrCaps(enabled));
                    return 0;
                }))
                .then(literal("add")
                        .then(argument("playerName", wordWithChars()).executes(c -> {
                            String playerName = getString(c, "playerName");
                            Optional<MinetoolsUuidResponse> result = MinetoolsApi.INSTANCE.getProfileFromUsername(playerName);
                            if (result.isEmpty()) {
                                c.getSource().getEmbed().title("Invalid username: " + playerName);
                                return 0;
                            }
                            UUID uuid = result.get().uuid();
                            if (PLUGIN_CONFIG.whitelist.containsKey(uuid)) {
                                c.getSource().getEmbed().title(playerName + " is already whitelisted");
                                return 0;
                            }
                            PLUGIN_CONFIG.whitelist.put(uuid, new dev.zenith.pearlplus.PearlPlusConfig.WhitelistedPlayer(playerName, uuid));
                            c.getSource().getEmbed().title("Added " + playerName + " to whitelist");
                            LOG.info("Added " + playerName + " (" + uuid + ") to whitelist");
                            return 0;
                        })))
                .then(literal("remove")
                        .then(argument("playerName", wordWithChars()).executes(c -> {
                            String playerName = getString(c, "playerName");
                            Optional<MinetoolsUuidResponse> result = MinetoolsApi.INSTANCE.getProfileFromUsername(playerName);
                            if (result.isEmpty()) {
                                c.getSource().getEmbed().title("Invalid username: " + playerName);
                                return 0;
                            }
                            UUID uuid = result.get().uuid();
                            if (!PLUGIN_CONFIG.whitelist.containsKey(uuid)) {
                                c.getSource().getEmbed().title(playerName + " is not in the whitelist");
                                return 0;
                            }
                            PLUGIN_CONFIG.whitelist.remove(uuid);
                            c.getSource().getEmbed().title("Removed " + playerName + " from whitelist");
                            LOG.info("Removed " + playerName + " (" + uuid + ") from whitelist");
                            return 0;
                        })))
                .then(literal("list").executes(c -> {
                    if (PLUGIN_CONFIG.whitelist.isEmpty()) {
                        c.getSource().getEmbed().title("Whitelist is empty");
                        return 0;
                    }
                    StringBuilder sb = new StringBuilder();
                    for (dev.zenith.pearlplus.PearlPlusConfig.WhitelistedPlayer player : PLUGIN_CONFIG.whitelist.values()) {
                        sb.append("- ").append(player.username).append(" (").append(player.uuid).append(")\n");
                    }
                    c.getSource().getEmbed()
                            .title("Whitelist (" + PLUGIN_CONFIG.whitelist.size() + " players)")
                            .description(sb.toString().trim());
                    return 0;
                }))
                .then(literal("clear").executes(c -> {
                    int count = PLUGIN_CONFIG.whitelist.size();
                    PLUGIN_CONFIG.whitelist.clear();
                    c.getSource().getEmbed().title("Cleared whitelist (" + count + " players removed)");
                    LOG.info("Cleared whitelist (" + count + " players removed)");
                    return 0;
                })));
                
                builder.then(literal("droppearlafterload")
                .then(argument("toggle", toggle()).executes(c -> {
                    boolean dropPearlAfterLoad = getToggle(c, "toggle");
                    PLUGIN_CONFIG.autoLoad.dropPearlAfterLoad = dropPearlAfterLoad;
                    c.getSource().getEmbed()
                            .title("Drop pearl after load " + toggleStrCaps(dropPearlAfterLoad));
                    return 0;
                })));

        return builder;
    }

    @Override
    public void defaultEmbed(final Embed builder) {
        String defaultPearlId = PLUGIN_CONFIG.defaultPearlId == null ? "None" : PLUGIN_CONFIG.defaultPearlId;
        builder
                .addField("Enabled", toggleStr(PLUGIN_CONFIG.autoLoad.enabled))
                .addField("Default Pearl ID", defaultPearlId)
                .addField("Return Position", toggleStr(PLUGIN_CONFIG.autoLoad.returnToStartPos))
                .addField("Strict", toggleStr(!PLUGIN_CONFIG.autoLoad.allowNoiseAfterPearl))
                .addField("Autodetect", toggleStr(PLUGIN_CONFIG.autoDetect.enabled))
                .addField("Autodetect Temp", toggleStr(PLUGIN_CONFIG.autoDetect.temporaryMode))
                .addField("Distance Check", toggleStr(PLUGIN_CONFIG.autoDetect.distanceCheck))
                .addField("Auto Default", toggleStr(PLUGIN_CONFIG.autoLoad.autoDefaultToPresent))
                .addField("Whitelist", toggleStr(PLUGIN_CONFIG.autoLoad.whitelistEnabled))
                .addField("Drop Pearl After Load", toggleStr(PLUGIN_CONFIG.autoLoad.dropPearlAfterLoad))
                .primaryColor();
    }
}
