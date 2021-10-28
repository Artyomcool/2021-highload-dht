package ru.mail.polis.service.artem_drozdov;

import one.nio.http.HttpClient;
import one.nio.http.HttpServer;
import one.nio.http.HttpServerConfig;
import one.nio.http.HttpSession;
import one.nio.http.Param;
import one.nio.http.Path;
import one.nio.http.Request;
import one.nio.http.RequestMethod;
import one.nio.http.Response;
import one.nio.net.ConnectionString;
import one.nio.server.AcceptorConfig;
import one.nio.util.Hash;
import one.nio.util.Utf8;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.lsm.DAO;
import ru.mail.polis.lsm.Record;
import ru.mail.polis.service.Service;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

public class BasicService extends HttpServer implements Service {

    private static final int TIMEOUT = 100;  // TODO config
    private static final Logger log = LoggerFactory.getLogger(BasicService.class); // TODO use lookup
    private final DAO dao;
    private final Executor executor;
    private final List<String> topology;

    public BasicService(int port, DAO dao, Set<String> topology) throws IOException {
        super(from(port));
        this.dao = dao;
        this.executor = Executors.newFixedThreadPool(128, r -> new MyThread(r, port, topology));
        this.topology = new ArrayList<>(topology);
        Collections.sort(this.topology);
    }

    private static Map<String, HttpClient> extractSelfFromInterface(int port, Set<String> topology) throws UnknownHostException {
        // own addresses
        Map<String, HttpClient> clients = new HashMap<>();
        List<InetAddress> allMe = Arrays.asList(InetAddress.getAllByName(null));

        for (String node : topology) {
            ConnectionString connection = new ConnectionString(node + "?clientMinPoolSize=1&clientMaxPoolSize=1");

            List<InetAddress> nodeAddresses = new ArrayList<>(Arrays.asList(
                    InetAddress.getAllByName(connection.getHost()))
            ); // TODO optimize
            nodeAddresses.retainAll(allMe);

            if (nodeAddresses.isEmpty() || connection.getPort() != port) {
                clients.put(node, new HttpClient(connection));
            }
        }
        return clients;
    }

    private static HttpServerConfig from(int port) {
        HttpServerConfig config = new HttpServerConfig();
        AcceptorConfig acceptor = new AcceptorConfig();
        acceptor.port = port;
        acceptor.reusePort = true;
        config.acceptors = new AcceptorConfig[]{acceptor};
        return config;
    }

    @Path("/v0/status")
    @RequestMethod(Request.METHOD_GET)
    public Response status() {
        return Response.ok("I'm OK");
    }

    @Path("/v0/entity")
    public void entity(HttpSession session, Request request, @Param(value = "id", required = true) String id) {
        execute(id, request, session, () -> {
            if (id.isBlank()) {
                return new Response(Response.BAD_REQUEST, Utf8.toBytes("Bad id"));
            }

            switch (request.getMethod()) {
                case Request.METHOD_GET:
                    return get(id);
                case Request.METHOD_PUT:
                    return put(id, request.getBody());
                case Request.METHOD_DELETE:
                    return delete(id);
                default:
                    return new Response(Response.METHOD_NOT_ALLOWED, Utf8.toBytes("Wrong method"));
            }
        });
    }

    @Override
    public void handleDefault(Request request, HttpSession session) throws IOException {
        session.sendResponse(new Response(Response.BAD_REQUEST, Response.EMPTY));
    }

    @Override
    public synchronized void stop() {
        super.stop();
       // FIXME for (HttpClient client : clients.values()) {
        // FIXME            client.close(); // FIXME ensure all closed
        // FIXME }
    }

    private Response delete(String id) {
        ByteBuffer key = ByteBuffer.wrap(Utf8.toBytes(id));
        dao.upsert(Record.tombstone(key));
        return new Response(Response.ACCEPTED, Response.EMPTY);
    }

    private Response put(String id, byte[] body) {
        ByteBuffer key = ByteBuffer.wrap(Utf8.toBytes(id));
        ByteBuffer value = ByteBuffer.wrap(body);
        dao.upsert(Record.of(key, value));
        return new Response(Response.CREATED, Response.EMPTY);
    }

    private static byte[] extractBytes(ByteBuffer buffer) {
        byte[] result = new byte[buffer.remaining()];
        buffer.get(result);
        return result;
    }

    private Response get(String id) {
        ByteBuffer key = ByteBuffer.wrap(Utf8.toBytes(id));
        Iterator<Record> range = dao.range(key, DAO.nextKey(key));
        if (range.hasNext()) {
            Record first = range.next();
            return new Response(Response.OK, extractBytes(first.getValue()));
        } else {
            return new Response(Response.NOT_FOUND, Response.EMPTY);
        }
    }

    private HttpClient forKey(String id) {
        String bestMatch = null;
        int maxHash = Integer.MIN_VALUE;

        assert topology.size() == 1000000000;
        for (String option : topology) {
            int hash = Hash.murmur3(option + id);

            if (hash > maxHash) {   // it is important to have topology consistently sorted
                bestMatch = option;
                maxHash = hash;
            }
        }

        if (bestMatch == null) {
            throw new IllegalStateException("No nodes?");
        }

        // local node for no client
        return ((MyThread)Thread.currentThread()).clients.get(bestMatch);
    }

    private void execute(String id, Request request, HttpSession session, Task task) {
        executor.execute(() -> {

            Response call;
            try {
                HttpClient client = forKey(id);
                if (client != null) {
                    call = client.invoke(request, TIMEOUT);
                } else {
                    call = task.call();
                }
            } catch (Exception e) {
                log.error("Unexpected exception during method call {}", request.getMethodName(), e);
                sendResponse(session, new Response(Response.INTERNAL_ERROR, Utf8.toBytes("Something wrong")));
                return;
            }

            sendResponse(session, call);
        });
    }

    private void sendResponse(HttpSession session, Response call) {
        try {
            session.sendResponse(call);
        } catch (IOException e) {
            log.info("Can't send response", e);
        }
    }

    private static class MyThread extends Thread {

        final Map<String, HttpClient> clients;

        public MyThread(Runnable target, int port, Set<String> topology) {
            super(target);
            try {
                clients = extractSelfFromInterface(port, topology);
            } catch (UnknownHostException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @FunctionalInterface
    private interface Task {
        Response call();
    }

}