package ru.mail.polis.service.artem_drozdov;

import one.nio.http.HttpServer;
import one.nio.http.HttpServerConfig;
import one.nio.http.HttpSession;
import one.nio.http.Param;
import one.nio.http.Path;
import one.nio.http.Request;
import one.nio.http.RequestMethod;
import one.nio.http.Response;
import one.nio.server.AcceptorConfig;
import one.nio.util.Utf8;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.lsm.DAO;
import ru.mail.polis.lsm.Record;
import ru.mail.polis.service.Service;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.concurrent.Executor;

public class BasicService extends HttpServer implements Service {

    private final Logger log = LoggerFactory.getLogger(BasicService.class); // TODO use lookup
    private final DAO dao;
    private final Executor executor;

    public BasicService(int port, DAO dao, Executor executor) throws IOException {
        super(from(port));
        this.dao = dao;
        this.executor = executor;
    }

    private static HttpServerConfig from( int port) {
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
    public void entity(HttpSession session, Request request, @Param(value = "id", required = true)  String id) {
        execute(request, session, () -> {
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
    public void handleDefault(
             Request request,
             HttpSession session) throws IOException {
        session.sendResponse(new Response(Response.BAD_REQUEST, Response.EMPTY));
    }

    private Response delete( String id) {
        ByteBuffer key = ByteBuffer.wrap(Utf8.toBytes(id));
        dao.upsert(Record.tombstone(key));
        return new Response(Response.ACCEPTED, Response.EMPTY);
    }

    private Response put( String id, byte[] body) {
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

    private void execute(Request request, HttpSession session, Task task) {
        executor.execute(() -> {
            Response call;
            try {
                call = task.call();
            } catch (Exception e) {
                log.error("Unexpected exception during method call {}", request.getMethodName(), e);
                BasicService.this.sendResponse(session, new Response(Response.INTERNAL_ERROR, Utf8.toBytes("Something wrong")));
                return;
            }

            BasicService.this.sendResponse(session, call);
        });
    }

    private void sendResponse(HttpSession session, Response call) {
        try {
            session.sendResponse(call);
        } catch (IOException e) {
            log.info("Can't send response", e);
        }
    }

    @FunctionalInterface
    private interface Task {
        Response call();
    }

}