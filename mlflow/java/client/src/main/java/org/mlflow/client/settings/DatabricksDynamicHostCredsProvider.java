package org.mlflow.client.settings;

import org.apache.log4j.Logger;

import javax.swing.text.html.Option;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;

public class DatabricksDynamicHostCredsProvider implements MlflowHostCredsProvider {
  private static final Logger logger = Logger.getLogger(DatabricksDynamicHostCredsProvider.class);

  private final Supplier<Map<String, String>> configProvider;

  private DatabricksDynamicHostCredsProvider(
    Supplier<Map<String, String>> configProvider) {
    this.configProvider = configProvider;
  }

  public static DatabricksDynamicHostCredsProvider createIfAvailable() {
    try {
      Class<?> cls = Class.forName("com.databricks.config.DatabricksClientSettingsProvider");
      return new DatabricksDynamicHostCredsProvider(
        (Supplier<Map<String, String>>) cls.newInstance());
    } catch (ClassNotFoundException e) {
      return null;
    } catch (IllegalAccessException | InstantiationException e) {
      logger.warn("Found but failed to invoke dynamic config provider", e);
      return null;
    }
  }

  @Override
  public MlflowHostCreds getHostCreds() {
    Map<String, String> config = configProvider.get();
    return new BasicMlflowHostCreds(
      config.get("host"),
      config.get("username"),
      config.get("password"),
      config.get("token"),
      "true".equals(config.get("no-tls-verify"))
    );
  }

  @Override
  public void refresh() {
    // no-op
  }
}
