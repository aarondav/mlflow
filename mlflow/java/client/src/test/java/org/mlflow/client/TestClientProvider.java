package org.mlflow.client;

import com.google.protobuf.Api;
import org.apache.log4j.Logger;

import java.io.*;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

class TestClientProvider {
  private static final Logger logger = Logger.getLogger(TestClientProvider.class);

  private Process serverProcess;

  ApiClient initializeClientAndServer() throws IOException {
    if (serverProcess != null) {
      throw new IllegalStateException("Previous server process not cleaned up");
    }

    String trackingUri = System.getenv("MLFLOW_TRACKING_URI");
    if (trackingUri != null) {
      logger.info("MLFLOW_TRACKING_URI was set, test will run against that server");
      return ApiClient.fromTrackingUri(trackingUri);
    } else {
      return startServerProcess();
    }
  }

  void cleanupClientAndServer() throws InterruptedException {
    if (serverProcess != null) {
      serverProcess.destroy();
      serverProcess.waitFor(30, TimeUnit.SECONDS);
      serverProcess = null;
    }
  }

  private ApiClient startServerProcess() throws IOException {
    ProcessBuilder pb = new ProcessBuilder();
    Path tempDir = Files.createTempDirectory(getClass().getSimpleName());
    int freePort = getFreePort();
    String bindAddress = "127.0.0.1";
    pb.command("mlflow", "server", "--host", bindAddress, "--port", "" + freePort,
      "--file-store", tempDir.toString());
    serverProcess = pb.start();

    drainStream(serverProcess.getInputStream(), System.out, "mlflow-server-stdout-reader");
    drainStream(serverProcess.getErrorStream(), System.err, "mlflow-server-stderr-reader");

    logger.info("Awaiting start of server on port " + freePort);
    long startTime = System.nanoTime();
    long maxWaitTimeSeconds = 30;
    while (System.nanoTime() - startTime < maxWaitTimeSeconds * 1000 * 1000 * 1000) {
      if (isPortOpen(bindAddress, freePort, 1)) {
        break;
      }
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
    if (!isPortOpen(bindAddress, freePort, 1)) {
      serverProcess.destroy();
      throw new IllegalStateException("Server failed to start on port " + freePort + " after "
        + maxWaitTimeSeconds + " seconds.");
    }

    return ApiClient.fromTrackingUri("http://" + bindAddress + ":" + freePort);
  }

  private void drainStream(InputStream inStream, PrintStream outStream, String threadName) {
    Thread drainThread = new Thread(threadName) {
      @Override
      public void run() {
        BufferedReader reader = new BufferedReader(new InputStreamReader(inStream,
          StandardCharsets.UTF_8));
        reader.lines().forEach(outStream::println);
        logger.info("Drain completed on " + threadName);
      }
    };
    drainThread.setDaemon(true);
    drainThread.start();
  }

  private int getFreePort() throws IOException {
    ServerSocket sock = new ServerSocket(0);
    int port = sock.getLocalPort();
    sock.close();
    return port;
  }


  private boolean isPortOpen(String host, int port, int timeoutSeconds) {
    try {
      String ip = InetAddress.getByName(host).getHostAddress();
      Socket socket = new Socket();
      socket.connect(new InetSocketAddress(ip, port), timeoutSeconds * 1000);
      socket.close();
    } catch (IOException e) {
      return false;
    }
    return true;
  }
}
