package dev.zenith.pearlplus;

import com.zenith.plugin.api.PluginAPI;
import com.zenith.plugin.api.Plugin;
import com.zenith.plugin.api.ZenithProxyPlugin;
import net.kyori.adventure.text.logger.slf4j.ComponentLogger;
import dev.zenith.pearlplus.command.*;
import dev.zenith.pearlplus.hydra.HydraIntegration;
import dev.zenith.pearlplus.hydra.HydraLedger;
import dev.zenith.pearlplus.module.*;

@Plugin(
    id = BuildConstants.PLUGIN_ID,
    version = BuildConstants.VERSION,
    description = "Slightly better pearl loading module.",
    url = "https://github.com/evilinc-labs/PearlPlus/",
    authors = {"duccss", "steve2b2t"},
    mcVersions = "*" // mark every version compatible
)

public class PearlPlusPlugin implements ZenithProxyPlugin {
    public static PluginAPI API;
    public static PearlPlusConfig PLUGIN_CONFIG;
    public static ComponentLogger LOG;
    public static final HydraLedger LEDGER = new HydraLedger();
    public static AutoDetectModule AUTO_DETECT;
    public static HydraIntegration HYDRA;

    @Override
    public void onLoad(PluginAPI pluginAPI) {
        API = pluginAPI;
        LOG = pluginAPI.getLogger();
        LOG.info("PearlPlus Plugin loading...");
        PLUGIN_CONFIG = API.registerConfig(BuildConstants.PLUGIN_ID, PearlPlusConfig.class);
        API.registerCommand(new PearlPlusCommand());
        API.registerModule(new AutoLoadModule());
        AUTO_DETECT = new AutoDetectModule();
        API.registerModule(AUTO_DETECT);

        // Optional Hydra C2 integration — activates only when HYDRA_RABBIT_URL and
        // HYDRA_AGENT_ID env vars are present. No-ops silently on standalone deployments.
        HYDRA = new HydraIntegration();
        API.registerModule(HYDRA);
        HYDRA.tryConnect();

        LOG.info("PearlPlus Plugin loaded!");
    }
}
