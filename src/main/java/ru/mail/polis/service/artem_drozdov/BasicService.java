package ru.mail.polis.service.artem_drozdov;

import one.nio.http.HttpClient;
import one.nio.http.HttpException;
import one.nio.http.HttpServer;
import one.nio.http.HttpServerConfig;
import one.nio.http.HttpSession;
import one.nio.http.Param;
import one.nio.http.Path;
import one.nio.http.Request;
import one.nio.http.RequestMethod;
import one.nio.http.Response;
import one.nio.net.ConnectionString;
import one.nio.pool.PoolException;
import one.nio.server.AcceptorConfig;
import one.nio.util.Hash;
import one.nio.util.Utf8;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.lsm.DAO;
import ru.mail.polis.lsm.Record;
import ru.mail.polis.service.Service;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

public class BasicService extends HttpServer implements Service {

    public static final String INTERNAL_REQUEST_HEADER = "X-Internal-Request: MySecretKey";
    public static final String TIMESTAMP_HEADER = "X-Entity-Timestamp: ";

    private static final int TIMEOUT = 1000;  // TODO config
    private static final Logger log = LoggerFactory.getLogger(BasicService.class); // TODO use lookup
    private final DAO dao;
    private final Executor executor;
    private final List<String> topology;
    private final Map<String, HttpClient> clients;

    public BasicService(int port, DAO dao, Set<String> topology) throws IOException {
        super(from(port));
        this.dao = dao;
        this.executor = Executors.newFixedThreadPool(16);
        this.topology = new ArrayList<>(topology);
        this.clients = extractSelfFromInterface(port, topology);
        Collections.sort(this.topology);
    }

    private static Map<String, HttpClient> extractSelfFromInterface(int port, Set<String> topology) throws UnknownHostException {
        // own addresses
        Map<String, HttpClient> clients = new HashMap<>();
        List<InetAddress> allMe = Arrays.asList(InetAddress.getAllByName(null));

        for (String node : topology) {
            ConnectionString connection = new ConnectionString(node);

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

    private Response mergeForExpectedCode(List<Response> responses, int expectedCode, int ackCount, String answer) {
        log.info("ackCount: {}", ackCount);

        for (Response response : responses) {
            if (response.getStatus() != expectedCode) {
                log.info("wrongStatus: {}", response.getHeader(INTERNAL_REQUEST_HEADER));
                // TODO some metrics?
                continue;
            }

            ackCount--;
            if (ackCount == 0) {
                return new Response(answer, Response.EMPTY);
            }
        }

        log.info("ackCountRemaining: {}", ackCount);
        return new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY);
    }

    private Response selectBetterValue(Response previous, Response candidate) {
        if (previous == null || previous.getBody() == null) {
            return candidate;
        }

        if (candidate.getBody() == null) {
            return previous;
        }

        return Arrays.compare(previous.getBody(), candidate.getBody()) > 0
                ? candidate
                : previous;
    }

    private long extractTimestamp(Response response) {
        String header = response.getHeader(TIMESTAMP_HEADER);
        log.info("header {}", header);
        return header == null ? Long.MIN_VALUE : Long.parseLong(header);
    }

    private long extractTimestamp(Request request) {
        String header = request.getHeader(TIMESTAMP_HEADER);
        return header == null ? Long.MIN_VALUE : Long.parseLong(header);
    }

    private Response mergeForTimestamp(List<Response> responses, int ackCount) {
        Response best = null;
        long bestTimestamp = Long.MIN_VALUE;
        for (Response response : responses) {
            switch (response.getStatus()) {
                case HttpURLConnection.HTTP_OK:
                case HttpURLConnection.HTTP_NOT_FOUND:
                    ackCount--;

                    long candidateTimestamp = extractTimestamp(response);
                    log.info("timestamp merge {}/{}", bestTimestamp, candidateTimestamp);
                    if (candidateTimestamp == bestTimestamp) {
                        best = selectBetterValue(best, response);
                    } else if (candidateTimestamp > bestTimestamp) {
                        best = response;
                        bestTimestamp = candidateTimestamp;
                    }
                    break;
                default:
            }
        }

        if (best == null || ackCount > 0) {
            return new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY);
        }

        return best;
    }

    private Task taskForMethod(String id, Request request, boolean internal, long timestamp) {
        switch (request.getMethod()) {
            case Request.METHOD_GET:
                return new Task() {
                    @Override
                    public Response invokeOnLocalStorage() {
                        return get(id, internal);
                    }

                    @Override
                    public Response mergeCollectedResults(List<Response> responses, int ackCount) {
                        return mergeForTimestamp(responses, ackCount);
                    }
                };
            case Request.METHOD_PUT:
                return new Task() {
                    @Override
                    public Response invokeOnLocalStorage() {
                        log.info("invokeOnLocalStorage");
                        Response put = put(id, request.getBody(), timestamp);
                        log.info("invokeOnLocalStorageEnd {}", put.getStatus());
                        return put;
                    }

                    @Override
                    public Response mergeCollectedResults(List<Response> responses, int ackCount) {
                        return mergeForExpectedCode(
                                responses,
                                HttpURLConnection.HTTP_CREATED,
                                ackCount,
                                Response.CREATED
                        );
                    }
                };
            case Request.METHOD_DELETE:
                return new Task() {
                    @Override
                    public Response invokeOnLocalStorage() {
                        return delete(id, timestamp);
                    }

                    @Override
                    public Response mergeCollectedResults(List<Response> responses, int ackCount) {
                        return mergeForExpectedCode(
                                responses,
                                HttpURLConnection.HTTP_ACCEPTED,
                                ackCount,
                                Response.ACCEPTED
                        );
                    }
                };
            default:
                return null;
        }
    }

    @Path("/v0/entity")
    public void entity(
            HttpSession session,
            Request request,
            @Param(value = "id", required = true) String id,
            @Param(value = "replicas") String replicas
    ) {
        boolean internal ="".equals(request.getHeader(INTERNAL_REQUEST_HEADER));
        log.info("Invoke: {}/{}", request.getHeader(INTERNAL_REQUEST_HEADER), internal);
        long timestamp;
        if (internal) {
            timestamp = extractTimestamp(request);
        } else {
            timestamp = System.currentTimeMillis();
            request.addHeader(TIMESTAMP_HEADER + timestamp);
        }
        log.info("entity {}/{}", request, timestamp);
        Task task = taskForMethod(id, request, internal, timestamp);

        execute(id, replicas, request, session, task, internal);
    }

    @Override
    public void handleDefault(Request request, HttpSession session) throws IOException {
        session.sendResponse(new Response(Response.BAD_REQUEST, Response.EMPTY));
    }

    @Override
    public synchronized void stop() {
        super.stop();
        for (HttpClient client : clients.values()) {
            client.close();
        }
    }

    private Response delete(String id, long timestamp) {
        ByteBuffer key = ByteBuffer.wrap(Utf8.toBytes(id));
        dao.upsert(Record.tombstone(key, timestamp));
        return new Response(Response.ACCEPTED, Response.EMPTY);
    }

    private Response put(String id, byte[] body, long timestamp) {
        ByteBuffer key = ByteBuffer.wrap(Utf8.toBytes(id));
        ByteBuffer value = ByteBuffer.wrap(body);
        dao.upsert(Record.of(key, value, timestamp));
        return new Response(Response.CREATED, Response.EMPTY);
    }

    private static byte[] extractBytes(ByteBuffer buffer) {
        byte[] result = new byte[buffer.remaining()];
        buffer.get(result);
        return result;
    }

    private Response okForGet(Record record, boolean includeTimestamp) {
        Response response;
        if (record.isTombstone()) {
            response = new Response(Response.NOT_FOUND, Response.EMPTY);
        } else {
            response = new Response(Response.OK, extractBytes(record.getValue()));
        }

        if (includeTimestamp) {
            response.addHeader(TIMESTAMP_HEADER + record.getTimestamp());
        }

        return response;
    }

    private Response get(String id, boolean internal) {
        ByteBuffer key = ByteBuffer.wrap(Utf8.toBytes(id));
        Iterator<Record> range = dao.range(key, DAO.nextKey(key), internal);
        if (range.hasNext()) {
            return okForGet(range.next(), internal);
        } else {
            return new Response(Response.NOT_FOUND, Response.EMPTY);
        }
    }

    private List<Node> forKey(String id, int nodeCount) {
        PriorityQueue<Node> nodes = new PriorityQueue<>(Comparator.comparingInt(o -> o.hash));

        for (String option : topology) {
            int hash = Hash.murmur3(option + id);

            HttpClient client = clients.get(option);
            Node node;
            if (client == null) {   // it is our local node
                node = createLocalNode(hash);
            } else {
                node = createRemoteNode(hash, client);
            }
            nodes.add(node);

            if (nodes.size() > nodeCount) {
                nodes.remove();
            }
        }

        return new ArrayList<>(nodes);
    }

    private Node createLocalNode(int hash) {
        return new Node(hash) {
            @Override
            Response invoke(Request request, Task task) {
                return task.invokeOnLocalStorage();
            }
        };
    }

    private Node createRemoteNode(int hash, HttpClient client) {
        return new Node(hash) {
            @Override
            Response invoke(Request request, Task task) throws InterruptedException {
                try {
                    return client.invoke(request, TIMEOUT);
                } catch (HttpException | IOException | PoolException e) {
                    log.debug("Invoke remote error", e);
                    return new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY);
                }
            }
        };
    }

    private void execute(String id, String replicas, Request request, HttpSession session, Task task, boolean internal) {
        if (task == null) {
            sendResponse(session, new Response(Response.METHOD_NOT_ALLOWED, Utf8.toBytes("Wrong method")));
            return;
        }

        executor.execute(() -> {

            Response result;

            try {
                result = invoke(id, replicas, request, task, internal);
            } catch (Exception e) {
                log.error("Unexpected exception during method call {}", request.getMethodName(), e);
                sendResponse(session, new Response(Response.INTERNAL_ERROR, Utf8.toBytes("Something wrong")));
                return;
            }

            sendResponse(session, result);
        });
    }

    private Response invoke(String id, String replicas, Request request, Task task, boolean internal) throws InterruptedException {
        if (internal) {
            // TODO validate if wrong id
            return task.invokeOnLocalStorage();
        }

        int ack;
        int from;
        if (replicas != null) {
            String[] parsed = replicas.split("/");
            ack = Integer.parseInt(parsed[0]);
            from = Integer.parseInt(parsed[1]);
        } else {
            from = topology.size();
            ack = from / 2 + 1;
        }

        if (ack <= 0 || ack > from || id == null || id.equals("")) {
            return new Response(Response.BAD_REQUEST, Response.EMPTY);
        }

        request.addHeader(INTERNAL_REQUEST_HEADER);
        List<Node> nodes = forKey(id, from);
        List<Response> responses = new ArrayList<>();
        for (Node node : nodes) {
            // TODO make it parallel
            Response response = node.invoke(request, task);
            log.info("invokeOnNode {}/{}", node.getClass(), response.getStatus());
            responses.add(response);
        }

        return task.mergeCollectedResults(responses, ack);
    }

    private void sendResponse(HttpSession session, Response call) {
        try {
            session.sendResponse(call);
        } catch (IOException e) {
            log.info("Can't send response", e);
        }
    }

    private interface Task {
        Response invokeOnLocalStorage();

        Response mergeCollectedResults(List<Response> responses, int ackCount);
    }

    private abstract static class Node {
        final int hash;

        private Node(int hash) {
            this.hash = hash;
        }

        abstract Response invoke(Request request, Task task) throws InterruptedException;

    }

}