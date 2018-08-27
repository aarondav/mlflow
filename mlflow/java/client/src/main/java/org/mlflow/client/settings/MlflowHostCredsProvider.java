package org.mlflow.client.settings;

public interface MlflowHostCredsProvider {
  MlflowHostCreds getHostCreds();

  void refresh();
}
