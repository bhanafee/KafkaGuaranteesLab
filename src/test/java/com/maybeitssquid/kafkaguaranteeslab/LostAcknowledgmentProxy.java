package com.maybeitssquid.kafkaguaranteeslab;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A TCP proxy between Kafka clients and a broker that can lose one acknowledgment. It forwards
 * everything in both directions until it is told to drop the next produce acknowledgment. It then
 * lets the next produce request through to the broker, which writes the records, and when the
 * broker's response arrives it closes the client's connection instead of forwarding it.
 *
 * <p>To the producer this is a network failure after the request was sent: it cannot know whether
 * the records were written, and its internal retry sends the batch again on a new connection.
 *
 * <p>The proxy reads only the framing of the Kafka protocol. Every request and response starts with
 * a 4-byte length. A request continues with a 2-byte API key, a 2-byte version and a 4-byte
 * correlation id. A response continues with the correlation id of its request.
 */
final class LostAcknowledgmentProxy implements AutoCloseable {

  private static final short PRODUCE = 0;

  private final ServerSocket server;
  private final Set<Socket> sockets = ConcurrentHashMap.newKeySet();
  private final AtomicBoolean armed = new AtomicBoolean();
  private final AtomicInteger dropped = new AtomicInteger();
  private volatile int brokerPort;

  LostAcknowledgmentProxy() throws IOException {
    server = new ServerSocket(0, 50, InetAddress.getByName("localhost"));
    daemon("lost-ack-proxy-accept", this::accept).start();
  }

  /** The port clients connect to, and the one the broker must advertise. */
  int port() {
    return server.getLocalPort();
  }

  /** Sets the port the broker really listens on. */
  void forwardTo(int port) {
    this.brokerPort = port;
  }

  /** Loses the acknowledgment of the next produce request that passes through. */
  void dropNextProduceAcknowledgment() {
    armed.set(true);
  }

  /** How many acknowledgments have been lost so far. */
  int dropped() {
    return dropped.get();
  }

  private void accept() {
    while (!server.isClosed()) {
      try {
        Socket client = server.accept();
        Socket broker = new Socket("localhost", brokerPort);
        sockets.add(client);
        sockets.add(broker);
        Set<Integer> toDrop = ConcurrentHashMap.newKeySet();
        daemon("lost-ack-proxy-requests", () -> pumpRequests(client, broker, toDrop)).start();
        daemon("lost-ack-proxy-responses", () -> pumpResponses(broker, client, toDrop)).start();
      } catch (IOException ex) {
        // The server socket was closed, or the broker is gone. Either way the loop ends or retries.
      }
    }
  }

  private void pumpRequests(Socket client, Socket broker, Set<Integer> toDrop) {
    try (client;
        broker) {
      var in = new DataInputStream(client.getInputStream());
      var out = new DataOutputStream(broker.getOutputStream());
      while (true) {
        byte[] frame = readFrame(in);
        ByteBuffer header = ByteBuffer.wrap(frame);
        if (header.getShort(0) == PRODUCE && armed.compareAndSet(true, false)) {
          toDrop.add(header.getInt(4));
        }
        writeFrame(out, frame);
      }
    } catch (IOException ex) {
      // One side closed the connection. Closing both sockets ends the other pump.
    }
  }

  private void pumpResponses(Socket broker, Socket client, Set<Integer> toDrop) {
    try (broker;
        client) {
      var in = new DataInputStream(broker.getInputStream());
      var out = new DataOutputStream(client.getOutputStream());
      while (true) {
        byte[] frame = readFrame(in);
        if (toDrop.remove(ByteBuffer.wrap(frame).getInt(0))) {
          // The broker has written the records. The producer never hears of it.
          dropped.incrementAndGet();
          return;
        }
        writeFrame(out, frame);
      }
    } catch (IOException ex) {
      // One side closed the connection. Closing both sockets ends the other pump.
    }
  }

  private static byte[] readFrame(DataInputStream in) throws IOException {
    byte[] frame = new byte[in.readInt()];
    in.readFully(frame);
    return frame;
  }

  private static void writeFrame(DataOutputStream out, byte[] frame) throws IOException {
    out.writeInt(frame.length);
    out.write(frame);
    out.flush();
  }

  private static Thread daemon(String name, Runnable task) {
    var thread = new Thread(task, name);
    thread.setDaemon(true);
    return thread;
  }

  @Override
  public void close() throws IOException {
    server.close();
    for (Socket socket : sockets) {
      socket.close();
    }
  }
}
