package org.mlflow.client;

import java.net.URI;
import java.util.*;

import org.apache.http.client.utils.URIBuilder;
import org.mlflow.api.proto.Service.*;
import org.mlflow.client.objects.BaseSearch;
import org.mlflow.client.objects.ObjectUtils;
import org.mlflow.client.objects.FromProtobufMapper;
import org.mlflow.client.objects.ToProtobufMapper;
import org.mlflow.client.settings.BasicClientSettingsProvider;
import org.mlflow.client.settings.ClientSettingsProvider;
import org.mlflow.client.settings.DatabricksConfigSettingsProvider;
import org.mlflow.client.settings.DatabricksDynamicClientSettingsProvider;

public class ApiClient {
  private String basePath = "api/2.0/preview/mlflow";
  private ToProtobufMapper toMapper = new ToProtobufMapper();
  private FromProtobufMapper fromMapper = new FromProtobufMapper();
  private HttpCaller httpCaller;

  private ClientSettingsProvider settings;

  private ApiClient(ClientSettingsProvider settings) {
    String apiUri = settings.getHost() + "/" + basePath;
    httpCaller = new HttpCaller(apiUri);
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
    ClientSettingsProvider provider;
    if ("databricks".equals(uri.getScheme())) {
      provider = DatabricksDynamicClientSettingsProvider.createIfAvailable();
      if (provider == null) {
        provider = new DatabricksConfigSettingsProvider(uri.getHost());
      }
    } else if (trackingUri.equals("databricks")) {
      provider = new DatabricksConfigSettingsProvider();
    } else if ("http".equals(uri.getScheme()) || "https".equals(uri.getScheme())) {
      provider = new BasicClientSettingsProvider(trackingUri);
    } else if (uri.getScheme() == null || "file".equals(uri.getScheme())) {
      throw new IllegalArgumentException("Java Client currently does not support" +
        " local tracking URIs. Please point to a Tracking Server.");
    } else {
      throw new IllegalArgumentException("Invalid tracking server uri: " + trackingUri);
    }
    return new ApiClient(provider);
  }

  public GetExperiment.Response getExperiment(long experimentId) throws Exception {
    URIBuilder builder = httpCaller.makeURIBuilder("experiments/get")
      .setParameter("experiment_id", "" + experimentId);
    return toMapper.toGetExperimentResponse(httpCaller._get(builder));
  }

  public List<Experiment> listExperiments() throws Exception {
    return toMapper.toListExperimentsResponse(httpCaller.get("experiments/list"))
      .getExperimentsList();
  }

  public long createExperiment(String experimentName) throws Exception {
    String ijson = fromMapper.makeCreateExperimentRequest(experimentName);
    String ojson = post("experiments/create", ijson);
    return toMapper.toCreateExperimentResponse(ojson).getExperimentId();
  }

  public Run getRun(String runUuid) throws Exception {
    URIBuilder builder = httpCaller.makeURIBuilder("runs/get").setParameter("run_uuid", runUuid);
    return toMapper.toGetRunResponse(httpCaller._get(builder)).getRun();
  }

  public RunInfo createRun(CreateRun request) throws Exception {
    String ijson = fromMapper.toJson(request);
    String ojson = post("runs/create", ijson);
    return toMapper.toCreateRunResponse(ojson).getRun().getInfo();
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
    URIBuilder builder = httpCaller.makeURIBuilder("metrics/get")
      .setParameter("run_uuid", runUuid)
      .setParameter("metric_key", metricKey);
    return toMapper.toGetMetricResponse(httpCaller._get(builder)).getMetric();
  }

  public List<Metric> getMetricHistory(String runUuid, String metricKey) throws Exception {
    URIBuilder builder = httpCaller.makeURIBuilder("metrics/get-history")
      .setParameter("run_uuid", runUuid)
      .setParameter("metric_key", metricKey);
    return toMapper.toGetMetricHistoryResponse(httpCaller._get(builder)).getMetricsList();
  }

  public ListArtifacts.Response listArtifacts(String runUuid, String path) throws Exception {
    URIBuilder builder = httpCaller.makeURIBuilder("artifacts/list")
      .setParameter("run_uuid", runUuid)
      .setParameter("path", path);
    return toMapper.toListArtifactsResponse(httpCaller._get(builder));
  }

  public byte[] getArtifact(String runUuid, String path) throws Exception {
    URIBuilder builder = httpCaller.makeURIBuilder("artifacts/get")
      .setParameter("run_uuid", runUuid)
      .setParameter("path", path);
    return httpCaller._getAsBytes(builder.toString());
  }

  public SearchRuns.Response search(long[] experimentIds, BaseSearch[] clauses)
    throws Exception {
    SearchRuns search = ObjectUtils.makeSearchRequest(experimentIds, clauses);
    String ijson = fromMapper.toJson(search);
    String ojson = post("runs/search", ijson);
    return toMapper.toSearchRunsResponse(ojson);
  }

  public Optional<Experiment> getExperimentByName(String experimentName) throws Exception {
    return listExperiments().stream().filter(e -> e.getName()
      .equals(experimentName)).findFirst();
  }

  public long getOrCreateExperimentId(String experimentName) throws Exception {
    Optional<Experiment> opt = getExperimentByName(experimentName);
    return opt.isPresent() ? opt.get().getExperimentId() : createExperiment(experimentName);
  }

  public String get(String path) throws Exception {
    return httpCaller.get(path);
  }

  public String post(String path, String json) throws Exception {
    return httpCaller.post(path, json);
  }
}
