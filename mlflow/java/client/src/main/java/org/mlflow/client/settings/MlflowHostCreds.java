package org.mlflow.client.settings;

public interface MlflowHostCreds {
  String getHost();

  String getUsername();

  String getPassword();

  String getToken();

  boolean getNoTlsVerify();
}
