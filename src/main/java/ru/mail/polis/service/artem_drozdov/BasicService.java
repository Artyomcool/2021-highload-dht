package ru.mail.polis.service.artem_drozdov;

import one.nio.http.HttpException;
import one.nio.http.HttpServer;
import one.nio.http.HttpServerConfig;
import one.nio.http.HttpSession;
import one.nio.http.Param;
import one.nio.http.Path;
import one.nio.http.Request;
import one.nio.http.RequestMethod;
import one.nio.http.Response;
import one.nio.net.Socket;
import one.nio.pool.PoolException;
import one.nio.server.AcceptorConfig;
import one.nio.server.RejectedSessionException;
import one.nio.util.Hash;
import one.nio.util.Utf8;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.lsm.DAO;
import ru.mail.polis.lsm.Record;
import ru.mail.polis.service.Service;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.function.Function;
import java.util.function.Supplier;

public class BasicService extends HttpServer implements Service {

    // FIXME parse headers
    public static final String INTERNAL_REQUEST_HEADER = "X-Internal-Request";
    public static final String INTERNAL_REQUEST_HEADER_VALUE = "MySecretKey";
    public static final String INTERNAL_REQUEST_CHECK = INTERNAL_REQUEST_HEADER + ": " + INTERNAL_REQUEST_HEADER_VALUE;
    public static final String TIMESTAMP_HEADER = "X-Entity-Timestamp";
    public static final String TIMESTAMP_HEADER_FOR_READ = TIMESTAMP_HEADER + ": ";

    private static final Logger log = LoggerFactory.getLogger(BasicService.class); // TODO use lookup
    public static final byte[] NEXT_LINE = {(byte) '\n'};
    private final DAO dao;
    private final Executor executor;
    private final List<String> topology;
    private final String self;

    private final Executor httpExecutor = new ForkJoinPool(16);
    private final HttpClient client;


    public BasicService(int port, DAO dao, Set<String> topology) throws IOException {
        super(from(port));
        this.dao = dao;
        this.executor = Executors.newFixedThreadPool(16);
        this.topology = new ArrayList<>(topology);
        this.self = extractSelf(port, topology);
        Collections.sort(this.topology);

        client = HttpClient.newBuilder()
                .executor(httpExecutor)
                .version(HttpClient.Version.HTTP_1_1)
                .build();    // TODO configure
    }

    private static HttpServerConfig from(int port) {
        HttpServerConfig config = new HttpServerConfig();
        AcceptorConfig acceptor = new AcceptorConfig();
        acceptor.port = port;
        acceptor.reusePort = true;
        config.acceptors = new AcceptorConfig[]{acceptor};
        return config;
    }

    private static String extractSelf(int selfPort, Collection<String> topology) {
        for (String option : topology) {
            try {
                int port = new URI(option).getPort();
                if (port == selfPort) {
                    return option;
                }
            } catch (URISyntaxException e) {
                throw new IllegalArgumentException("Wrong url", e);
            }
        }
        throw new IllegalArgumentException("Unknown port " + selfPort);
    }

    @Override
    public HttpSession createSession(Socket socket) throws RejectedSessionException {
        return new MyHttpSession(socket, this);
    }

    @Path("/v0/entities")
    public void entities(
            HttpSession session,
            @Param(value = "start", required = true) String start,
            @Param(value = "end") String end
    ) {
        if (start.isEmpty()) {
            log.warn("Start is empty");
            sendResponse(session, new Response(Response.BAD_REQUEST, Utf8.toBytes("Start is empty")));
            return;
        }
        MyHttpSession myHttpSession = (MyHttpSession) session;
        Response response = new Response(Response.OK);
        response.addHeader("Transfer-Encoding: chunked");
        try {
            ByteBuffer from = ByteBuffer.wrap(Utf8.toBytes(start));
            ByteBuffer to = end == null || end.isEmpty() ? null : ByteBuffer.wrap(Utf8.toBytes(end));
            Iterator<Record> range = dao.range(from, to, false);
            myHttpSession.sendResponseWithSupplier(response, new Supplier<byte[]>() {
                int state = 0;
                Record current = null;
                @Override
                public byte[] get() {
                    if (state == -1) {
                        return null;
                    }
                    if (current == null) {
                        if (!range.hasNext()) {
                            state = -1;
                            return "0\r\n\r\n".getBytes(StandardCharsets.US_ASCII);
                        }
                        current = range.next();
                    }
                    state = (state + 1) % 5;
                    switch (state) {
                        case 1:
                            int size = current.getKeySize() + 1 + current.getValueSize();
                            String hex = String.format("%x\r\n", size);
                            return hex.getBytes(StandardCharsets.US_ASCII);
                        case 2:
                            return extractBytes(current.getKey());
                        case 3:
                            // FIXME it looks like it is less effective than have local buffer
                            return NEXT_LINE;
                        case 4:
                            return extractBytes(current.getValue());
                        case 0:
                            current = null;
                            return "\r\n".getBytes(StandardCharsets.US_ASCII);
                        default:
                            throw new IllegalStateException();
                    }
                }
            });
        } catch (IOException e) {
            log.warn("What?", e);   // TODO deal with it pls
        }
    }

    @Path("/v0/natural")
    public void natural(HttpSession session) {
        MyHttpSession myHttpSession = (MyHttpSession) session;
        Response response = new Response(Response.OK);
        try {
            myHttpSession.sendResponseWithSupplier(response, new Supplier<>() {
                int current = 1;

                @Override
                public byte[] get() {
                    String data = Integer.toHexString(current) + "\n";
                    current++;
                    if (current == 10000) {
                        return null;
                    }
                    return data.getBytes(StandardCharsets.US_ASCII);
                }
            });
        } catch (IOException e) {
            log.warn("What?", e);   // TODO deal with it pls
        }
    }

    @Path("/v0/status")
    @RequestMethod(Request.METHOD_GET)
    public Response status() {
        return Response.ok("I'm OK");
    }

    private Response mergeForExpectedCode(List<RemoteData> responses, int expectedCode, int ackCount, String answer) {
        log.info("ackCount: {}", ackCount);

        for (RemoteData response : responses) {
            if (response.status != expectedCode) {
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

    private RemoteData selectBetterValue(RemoteData previous, RemoteData candidate) {
        if (previous == null || previous.data == null) {
            return candidate;
        }

        if (candidate.data == null) {
            return previous;
        }

        return Arrays.compare(previous.data, candidate.data) > 0
                ? candidate
                : previous;
    }

    private long extractTimestamp(Request request) {
        String header = request.getHeader(TIMESTAMP_HEADER_FOR_READ);
        return header == null ? Long.MIN_VALUE : Long.parseLong(header);
    }

    private Response mergeForTimestamp(List<RemoteData> responses, int ackCount) {
        RemoteData best = null;
        long bestTimestamp = Long.MIN_VALUE;
        for (RemoteData response : responses) {
            switch (response.status) {
                case HttpURLConnection.HTTP_OK:
                case HttpURLConnection.HTTP_NOT_FOUND:
                    ackCount--;

                    long candidateTimestamp = response.timestamp == null ? Long.MIN_VALUE : response.timestamp;
                    log.info("timestamp merge {}/{}", bestTimestamp, candidateTimestamp);
                    if (candidateTimestamp == bestTimestamp) {
                        best = selectBetterValue(best, response);
                    } else if (candidateTimestamp > bestTimestamp) {
                        best = response;
                        bestTimestamp = candidateTimestamp;
                    }
                    break;
                default:
                    ///??
            }
        }

        if (best == null || ackCount > 0) {
            return new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY);
        }

        return toResponse(best);
    }

    @Path("/v0/entity")
    public void entity(
            HttpSession session,
            Request request,
            @Param(value = "id", required = true) String id,
            @Param(value = "replicas") String replicas
    ) {

        if (id.isEmpty()) {
            log.warn("Id is empty");
            sendResponse(session, new Response(Response.BAD_REQUEST, Utf8.toBytes("Id is empty")));
            return;
        }

        Context context;
        try {
            context = new Context(request, id, replicas);
        } catch (UnsupportedOperationException e) {
            log.warn("Wrong method", e);
            sendResponse(session, new Response(Response.BAD_REQUEST, Utf8.toBytes("Wrong method")));
            return;
        } catch (IllegalArgumentException e) {
            log.warn("Wrong argument", e);
            sendResponse(session, new Response(Response.BAD_REQUEST, Utf8.toBytes("Wrong arguments")));
            return;
        }

        execute(session, context);
    }

    private Replicas extractReplicas(String replicas) {
        if (replicas == null) {
            int from = topology.size();
            int ack = from / 2 + 1;
            return new Replicas(ack, from);
        }

        String[] parsed = replicas.split("/");
        int ack = Integer.parseInt(parsed[0]);
        int from = Integer.parseInt(parsed[1]);

        if (ack <= 0 || ack > from || from > topology.size()) {
            throw new IllegalArgumentException("Replicas wrong");
        }

        return new Replicas(ack, from);
    }

    @Override
    public void handleDefault(Request request, HttpSession session) throws IOException {
        session.sendResponse(new Response(Response.BAD_REQUEST, Response.EMPTY));
    }

    @Override
    public synchronized void stop() {
        super.stop();
    }

    private RemoteData delete(String id, long timestamp) {
        ByteBuffer key = ByteBuffer.wrap(Utf8.toBytes(id));
        dao.upsert(Record.tombstone(key, timestamp));
        return new RemoteData(202, Response.EMPTY, null);
    }

    private RemoteData upsert(String id, byte[] body, long timestamp) {
        ByteBuffer key = ByteBuffer.wrap(Utf8.toBytes(id));
        ByteBuffer value = ByteBuffer.wrap(body);
        dao.upsert(Record.of(key, value, timestamp));
        return new RemoteData(201, Response.EMPTY, null);
    }

    private static byte[] extractBytes(ByteBuffer buffer) {
        byte[] result = new byte[buffer.remaining()];
        buffer.get(result);
        return result;
    }

    private RemoteData okForGet(Record record) {
        if (record.isTombstone()) {
            return new RemoteData(404, null, record.getTimestamp());
        }
        return new RemoteData(
                200,
                extractBytes(record.getValue()),
                record.getTimestamp()
        );
    }

    private RemoteData get(String id) {
        ByteBuffer key = ByteBuffer.wrap(Utf8.toBytes(id));
        Iterator<Record> range = dao.range(key, DAO.nextKey(key), true);
        if (range.hasNext()) {
            return okForGet(range.next());
        } else {
            return new RemoteData(404, null, null);
        }
    }

    private List<Node> collectNodesForKey(String id, int nodeCount) {
        PriorityQueue<Node> nodes = new PriorityQueue<>(Comparator.comparingInt(o -> o.hash));

        for (String option : topology) {
            int hash = Hash.murmur3(option + id);

            Node node = new Node(hash, option.equals(self) ? null : option);
            nodes.add(node);

            if (nodes.size() > nodeCount) {
                nodes.remove();
            }
        }

        return new ArrayList<>(nodes);
    }

    private void execute(HttpSession session, Context context) {
        executor.execute(() -> {
            CompletableFuture<Response> result;

            result = context.isCoordinator
                    ? collectResponses(context)
                    : toInternalResponse(invokeLocalRequest(context));

            result.whenCompleteAsync((response, throwable) -> {
                try {
                    if (throwable != null) {
                        throw throwable;
                    }
                    sendResponse(session, response);
                } catch (RuntimeException | Error e) {
                    log.error("Unexpected error", e);
                    throw e;
                } catch (Exception e) {
                    log.error("Unexpected error", e);
                } catch (Throwable e) {
                    throw new Error(e);
                }
            }, executor);
        });
    }

    private CompletableFuture<Response> collectResponses(Context context) {
        List<Node> nodes = collectNodesForKey(context.id, context.replicas.from);
        List<CompletableFuture<RemoteData>> responses = new ArrayList<>();
        for (Node node : nodes) {
            if (node.isLocalNode()) {
                log.info("Execute on local node");
                responses.add(invokeLocalRequest(context));
            } else {
                try {
                    log.info("Execute on from remote node");
                    responses.add(invokeRemoteRequest(context, node));
                } catch (HttpException | IOException | PoolException e) {
                    log.warn("Exception during node communication", e);
                }
            }
        }

        return merge(context, responses);
    }

    private CompletableFuture<RemoteData> invokeLocalRequest(Context context) {
        return CompletableFuture.supplyAsync(() -> {
            switch (context.operation) {
                case GET:
                    return get(context.id);
                case UPSERT:
                    return upsert(context.id, context.payload, context.timestamp);
                case DELETE:
                    return delete(context.id, context.timestamp);
                default:
                    throw new UnsupportedOperationException("Unsupported operation: " + context.operation);
            }
        }, Runnable::run); // TODO use async after profiling
    }

    private CompletableFuture<RemoteData> invokeRemoteRequest(Context context, Node node) throws HttpException, IOException, PoolException {
        URI uri = URI.create(node.uri + "/v0/entity?id=" + context.id);
        log.info("URI {}", uri);
        switch (context.operation) {
            case GET: {
                HttpRequest request = HttpRequest.newBuilder(uri)
                        .timeout(Duration.ofMillis(1000))   // FIXME config
                        .GET()
                        .header(INTERNAL_REQUEST_HEADER, INTERNAL_REQUEST_HEADER_VALUE)
                        .build();

                CompletableFuture<HttpResponse<byte[]>> response =
                        client.sendAsync(request, HttpResponse.BodyHandlers.ofByteArray());

                return response.thenApply(httpResponse -> {
                    String getTime = httpResponse.headers().firstValue(TIMESTAMP_HEADER).orElse(null);
                    return new RemoteData(
                            httpResponse.statusCode(),
                            httpResponse.body(),
                            getTime == null ? null : Long.valueOf(getTime)
                    );
                });
            }

            case UPSERT: {
                HttpRequest put = HttpRequest.newBuilder(uri)
                        .timeout(Duration.ofMillis(1000))   // FIXME config
                        .PUT(HttpRequest.BodyPublishers.ofByteArray(context.payload))
                        .header(INTERNAL_REQUEST_HEADER, INTERNAL_REQUEST_HEADER_VALUE)
                        .header(TIMESTAMP_HEADER, String.valueOf(context.timestamp))
                        .build();

                CompletableFuture<HttpResponse<Void>> response =
                        client.sendAsync(put, HttpResponse.BodyHandlers.discarding());
                return response.thenApply(httpResponse -> new RemoteData(
                        httpResponse.statusCode(),
                        null,
                        null
                ));
            }
            case DELETE: {
                HttpRequest request = HttpRequest.newBuilder(uri)
                        .timeout(Duration.ofMillis(1000))   // FIXME config
                        .DELETE()
                        .header(INTERNAL_REQUEST_HEADER, INTERNAL_REQUEST_HEADER_VALUE)
                        .header(TIMESTAMP_HEADER, String.valueOf(context.timestamp))
                        .build();

                CompletableFuture<HttpResponse<Void>> response =
                        client.sendAsync(request, HttpResponse.BodyHandlers.discarding());
                return response.thenApply(httpResponse -> new RemoteData(
                        httpResponse.statusCode(),
                        null,
                        null
                ));
            }
            default:
                throw new UnsupportedOperationException("Unsupported operation: " + context.operation);
        }
    }

    private CompletableFuture<Response> merge(Context context, List<CompletableFuture<RemoteData>> responses) {

        CompletableFuture<Void> result = new CompletableFuture<>();

        AtomicReferenceArray<RemoteData> unwrappedResponses = new AtomicReferenceArray<>(responses.size());
        AtomicInteger acceptedCount = new AtomicInteger();
        AtomicInteger acceptableErrorCount = new AtomicInteger(context.replicas.from - context.replicas.ack);

        int index = 0;
        for (CompletableFuture<RemoteData> response : responses) {
            int current = (index++);
            response.handleAsync((remoteData, throwable) -> {
                log.debug("Response {}, {}", current, remoteData, throwable);
                if (throwable != null) {
                    try {
                        throwable = throwable instanceof CompletionException ? throwable.getCause() : throwable;
                        throw throwable;
                    } catch (IOException e) {
                        log.debug("Node unavailable: node", e); // TODO find way go understand what the node here
                        if (acceptableErrorCount.getAndDecrement() == 0) {
                            result.complete(null);
                        }
                        return null;
                    } catch (Throwable e) {
                        log.error("Unexpected error while waiting for result", e);
                        result.completeExceptionally(e);
                        return null;
                    }
                }

                if (remoteData.status >= 500) {
                    log.debug("Node unavailable: status {}, node {}", remoteData.status, remoteData.nodeName);
                    if (acceptableErrorCount.getAndDecrement() == 0) {
                        result.complete(null);
                    }
                    return null;
                }

                unwrappedResponses.set(current, remoteData);
                int ack = context.replicas.ack;
                if (acceptedCount.incrementAndGet() == ack) {
                    result.complete(null);
                }
                return null;
            }, executor).exceptionally(throwable -> {
                // TODO why is suppressed exception here?
                result.completeExceptionally(throwable);
                return null;
            });
        }

        return result.thenApplyAsync(unused -> {
            if (acceptableErrorCount.get() < 0) {
                return new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY);
            }

            int ack = context.replicas.ack;
            List<RemoteData> results = new ArrayList<>(ack);
            for (int i = 0; i < unwrappedResponses.length(); i++) {
                RemoteData data = unwrappedResponses.get(i);
                if (data == null) {
                    continue;
                }

                results.add(data);
                if (results.size() == ack) {
                    break;
                }
            }

            return mergeSync(context, results);
        }, executor);
    }

    private Response mergeSync(Context context, List<RemoteData> responses) {
        switch (context.operation) {
            case GET:
                return mergeForTimestamp(responses, context.replicas.ack);
            case UPSERT:
                return mergeForExpectedCode(
                        responses,
                        HttpURLConnection.HTTP_CREATED,
                        context.replicas.ack,
                        Response.CREATED
                );
            case DELETE:
                return mergeForExpectedCode(
                        responses,
                        HttpURLConnection.HTTP_ACCEPTED,
                        context.replicas.ack,
                        Response.ACCEPTED
                );
            default:
                throw new UnsupportedOperationException("Unsupported operation: " + context.operation);
        }
    }

    private String statusToString(int status) {
        switch (status) {
            case 200:
                return Response.OK;
            case 201:
                return Response.CREATED;
            case 202:
                return Response.ACCEPTED;
            case 404:
                return Response.NOT_FOUND;
        }
        throw new IllegalArgumentException("Unknown status: " + status);
    }

    private void sendResponse(HttpSession session, Response response) {
        try {
            session.sendResponse(response);
        } catch (IOException e) {
            log.info("Can't send response", e);
        }
    }

    private Response toResponse(RemoteData data) {
        return new Response(
                statusToString(data.status),
                data.data == null ? Response.EMPTY : data.data
        );
    }

    private CompletableFuture<Response> toInternalResponse(CompletableFuture<RemoteData> data) {
        return data.thenApply(remoteData -> {
            Response response = toResponse(remoteData);
            if (remoteData.timestamp != null) {
                response.addHeader(TIMESTAMP_HEADER_FOR_READ + remoteData.timestamp);
            }
            return response;
        });
    }

    static class Node {
        final int hash;
        final String uri;

        Node(int hash, String uri) {
            this.hash = hash;
            this.uri = uri;
        }

        boolean isLocalNode() {
            return uri == null;
        }
    }

    private class Context {
        final String id;
        final boolean isCoordinator;
        final long timestamp;
        final byte[] payload;
        final Operation operation;
        final String uri;

        final Replicas replicas;

        Context(Request request, String id, String replicas) {
            this.id = id;

            this.isCoordinator = !"".equals(request.getHeader(INTERNAL_REQUEST_CHECK));
            this.timestamp = isCoordinator
                    ? System.currentTimeMillis()
                    : extractTimestamp(request);
            this.payload = request.getBody();
            this.uri = request.getURI();

            this.replicas = isCoordinator
                    ? extractReplicas(replicas)
                    : null;

            switch (request.getMethod()) {
                case Request.METHOD_GET:
                    this.operation = Operation.GET;
                    break;
                case Request.METHOD_PUT:
                    this.operation = Operation.UPSERT;
                    break;
                case Request.METHOD_DELETE:
                    this.operation = Operation.DELETE;
                    break;
                default:
                    throw new UnsupportedOperationException("Method " + request.getMethodName() + " is not supported");
            }


        }

    }

    private static class RemoteData {
        final String nodeName = "unknown"; // FIXME provide node name
        final int status;
        final byte[] data;
        final Long timestamp;

        RemoteData(int status, byte[] data, Long timestamp) {
            this.status = status;
            this.data = data;
            this.timestamp = timestamp;
        }
    }

    private static class Replicas {
        final int ack;
        final int from;

        Replicas(int ack, int from) {
            this.ack = ack;
            this.from = from;
        }
    }

    private enum Operation {
        GET, UPSERT, DELETE
    }

    private static class MyHttpSession extends HttpSession {

        Supplier<byte[]> dataSupplier;

        public MyHttpSession(Socket socket, HttpServer server) {
            super(socket, server);
        }

        public void sendResponseWithSupplier(Response response, Supplier<byte[]> supplier) throws IOException {
            this.dataSupplier = supplier;
            response.setBody(supplier.get());
            sendResponse(response);
            processChain();
        }

        @Override
        public synchronized void scheduleClose() {
            if (dataSupplier == null) {
                super.scheduleClose();
            }
        }

        @Override
        protected void processWrite() throws Exception {
            super.processWrite();
            processChain();
        }

        private void processChain() throws IOException {
            if (dataSupplier != null) {
                while (queueHead == null) {
                    byte[] bytes = dataSupplier.get();
                    if (bytes == null) {
                        // TODO support keep-alive
                        super.scheduleClose();
                        return;
                    }
                    write(bytes, 0, bytes.length);
                }
            }
        }
    }
}