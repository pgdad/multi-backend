// Sample Java backend (JDK only, virtual threads). Same contract as the other sample backends:
// GET /health -> 200; anything else -> JSON echo. Query knobs: delay_ms[.<name>], status[.<name>].
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;

public class Server {
    static final String NAME = env("BACKEND_NAME", "java");

    static String env(String k, String d) {
        String v = System.getenv(k);
        return v == null || v.isEmpty() ? d : v;
    }

    static Map<String, String> query(String raw) {
        Map<String, String> m = new HashMap<>();
        if (raw == null) return m;
        for (String part : raw.split("&")) {
            int i = part.indexOf('=');
            if (i > 0) m.put(URLDecoder.decode(part.substring(0, i), StandardCharsets.UTF_8),
                             URLDecoder.decode(part.substring(i + 1), StandardCharsets.UTF_8));
        }
        return m;
    }

    static int knob(Map<String, String> q, String key, int def) {
        for (String k : new String[]{key + "." + NAME, key}) {
            String v = q.get(k);
            if (v != null) {
                try { return Integer.parseInt(v); } catch (NumberFormatException ignored) { }
            }
        }
        return def;
    }

    static String esc(String s) {
        if (s == null) return "";
        StringBuilder b = new StringBuilder();
        for (char c : s.toCharArray()) {
            if (c == '"' || c == '\\') b.append('\\').append(c);
            else if (c < 0x20) b.append(String.format("\\u%04x", (int) c));
            else b.append(c);
        }
        return b.toString();
    }

    static void send(HttpExchange ex, int status, String json) throws IOException {
        byte[] body = json.getBytes(StandardCharsets.UTF_8);
        ex.getResponseHeaders().set("Content-Type", "application/json");
        ex.getResponseHeaders().set("X-Backend-Name", NAME);
        ex.getResponseHeaders().set("X-Backend-Runtime", "java");
        boolean head = ex.getRequestMethod().equals("HEAD");
        ex.sendResponseHeaders(status, head ? -1 : body.length);
        if (!head) try (OutputStream os = ex.getResponseBody()) { os.write(body); }
        ex.close();
    }

    static void handle(HttpExchange ex) throws IOException {
        long n;
        try (InputStream in = ex.getRequestBody()) { n = in.transferTo(OutputStream.nullOutputStream()); }
        String path = ex.getRequestURI().getPath();
        if (path.equals("/health")) {
            send(ex, 200, "{\"status\":\"ok\",\"backend\":\"" + esc(NAME) + "\"}");
            return;
        }
        String rawQuery = ex.getRequestURI().getRawQuery();
        Map<String, String> q = query(rawQuery);
        int delay = knob(q, "delay_ms", 0);
        if (delay > 0) {
            try { Thread.sleep(delay); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        }
        var h = ex.getRequestHeaders();
        String json = "{\"backend\":\"" + esc(NAME) + "\",\"runtime\":\"java\",\"method\":\"" + esc(ex.getRequestMethod())
            + "\",\"path\":\"" + esc(path) + "\",\"query\":\"" + esc(rawQuery) + "\",\"body_bytes\":" + n
            + ",\"request_id\":\"" + esc(h.getFirst("X-Request-Id")) + "\",\"config_version\":\"" + esc(h.getFirst("X-Mb-Config-Version"))
            + "\",\"fanout\":" + "true".equals(h.getFirst("X-Mb-Fanout")) + ",\"delay_ms\":" + delay
            + ",\"time\":\"" + Instant.now() + "\"}";
        send(ex, knob(q, "status", 200), json);
    }

    public static void main(String[] args) throws IOException {
        int port = Integer.parseInt(env("PORT", "8003"));
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", port), 1024);
        server.createContext("/", ex -> {
            try { handle(ex); } catch (Exception e) {
                try { send(ex, 500, "{\"error\":\"" + esc(e.toString()) + "\"}"); } catch (IOException ignored) { }
            }
        });
        server.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
        server.start();
        System.out.println("java backend " + NAME + " listening on 127.0.0.1:" + port);
    }
}
