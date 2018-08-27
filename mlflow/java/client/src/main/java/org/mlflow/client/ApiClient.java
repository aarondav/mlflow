package org.mlflow.client;

import org.apache.http.client.utils.URIBuilder;

import org.mlflow.api.proto.Service.*;
import org.mlflow.client.objects.FromProtobufMapper;
import org.mlflow.client.objects.ToProtobufMapper;
import org.mlflow.client.settings.*;

import java.net.URI;
import java.util.List;
import java.util.Optional;

public class ApiClient {
  private ToProtobufMapper toMapper = new ToProtobufMapper();
  private FromProtobufMapper fromMapper = new FromProtobufMapper();
  private HttpCaller httpCaller;

  private ApiClient(MlflowHostCredsProvider hostCredsProvider) {
    httpCaller = new HttpCaller(hostCredsProvider);
  }

  public static ApiClient defaultClient() {
    String defaultTrackingUri = System.getenv("MLFLOW_TRACKING_URI");
    if (defaultTrackingUri == null) {
      throw new IllegalStateException("Default client requires MLFLOW_TRACKING_URI is set." +
        " Use fromTrackingUri() instead.");
    }
    return fromTrackingUri(defaultTrackingUri);
  }

  public static ApiClient fromTrackingUri(String trackingUri) {
    URI uri = URI.create(trackingUri);
    MlflowHostCredsProvider provider;
    if (trackingUri.equals("databricks")) {
      MlflowHostCredsProvider profileProvider = new DatabricksConfigHostCredsProvier();
      MlflowHostCredsProvider dynamicProvider = DatabricksDynamicHostCredsProvider.createIfAvailable();
      if (dynamicProvider != null) {
        provider = new HostCredsProviderChain(dynamicProvider, profileProvider);
      } else {
        provider = profileProvider;
      }
    } else if ("databricks".equals(uri.getScheme())) {
      provider = new DatabricksConfigHostCredsProvier(uri.getHost());
    } else if ("http".equals(uri.getScheme()) || "https".equals(uri.getScheme())) {
      provider = new BasicMlflowHostCreds(trackingUri);
    } else if (uri.getScheme() == null || "file".equals(uri.getScheme())) {
      throw new IllegalArgumentException("Java Client currently does not support" +
        " local tracking URIs. Please point to a Tracking Server.");
    } else {
      throw new IllegalArgumentException("Invalid tracking server uri: " + trackingUri);
    }
    return new ApiClient(provider);
  }

  public GetExperiment.Response getExperiment(long experimentId) throws Exception {
    URIBuilder builder = new URIBuilder("experiments/get")
      .setParameter("experiment_id", "" + experimentId);
    return toMapper.toGetExperimentResponse(httpCaller.get(builder.toString()));
  }

  public List<Experiment> listExperiments() throws Exception {
    return toMapper.toListExperimentsResponse(httpCaller.get("experiments/list"))
      .getExperimentsList();
  }

  public long createExperiment(String experimentName) throws Exception {
    String ijson = fromMapper.makeCreateExperimentRequest(experimentName);
    String ojson = httpCaller.post("experiments/create", ijson);
    return toMapper.toCreateExperimentResponse(ojson).getExperimentId();
  }

  public Run getRun(String runUuid) throws Exception {
    URIBuilder builder = new URIBuilder("runs/get").setParameter("run_uuid", runUuid);
    return toMapper.toGetRunResponse(httpCaller.get(builder.toString())).getRun();
  }

  public RunInfo createRun(CreateRun request) throws Exception {
    String ijson = fromMapper.toJson(request);
    String ojson = post("runs/create", ijson);
    return toMapper.toCreateRunResponse(ojson).getRun().getInfo();
  }

  public RunInfo createRun() throws Exception {
    return createRun(CreateRun.newBuilder().build());
  }

  public void updateRun(String runUuid, RunStatus status, long endTime) throws Exception {
    post("runs/update", fromMapper.makeUpdateRun(runUuid, status, endTime));
  }

  public void logParameter(String runUuid, String key, String value) throws Exception {
    post("runs/log-parameter", fromMapper.makeLogParam(runUuid, key, value));
  }

  public void logMetric(String runUuid, String key, float value) throws Exception {
    post("runs/log-metric", fromMapper.makeLogMetric(runUuid, key, value));
  }

  public Metric getMetric(String runUuid, String metricKey) throws Exception {
    URIBuilder builder = new URIBuilder("metrics/get")
      .setParameter("run_uuid", runUuid)
      .setParameter("metric_key", metricKey);
    return toMapper.toGetMetricResponse(httpCaller.get(builder.toString())).getMetric();
  }

  public List<Metric> getMetricHistory(String runUuid, String metricKey) throws Exception {
    URIBuilder builder = new URIBuilder("metrics/get-history")
      .setParameter("run_uuid", runUuid)
      .setParameter("metric_key", metricKey);
    return toMapper.toGetMetricHistoryResponse(httpCaller.get(builder.toString())).getMetricsList();
  }

  public ListArtifacts.Response listArtifacts(String runUuid, String path) throws Exception {
    URIBuilder builder = new URIBuilder("artifacts/list")
      .setParameter("run_uuid", runUuid)
      .setParameter("path", path);
    return toMapper.toListArtifactsResponse(httpCaller.get(builder.toString()));
  }

  public byte[] getArtifact(String runUuid, String path) throws Exception {
    URIBuilder builder = new URIBuilder("artifacts/get")
      .setParameter("run_uuid", runUuid)
      .setParameter("path", path);
    return httpCaller.getAsBytes(builder.toString());
  }

  public Optional<Experiment> getExperimentByName(String experimentName) throws Exception {
    return listExperiments().stream().filter(e -> e.getName()
      .equals(experimentName)).findFirst();
  }

  public String get(String path) throws Exception {
    return httpCaller.get(path);
  }

  public String post(String path, String json) throws Exception {
    return httpCaller.post(path, json);
  }
}
