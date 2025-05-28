package edu.sustech.cs307.http;

import com.fasterxml.jackson.databind.ObjectMapper;
import edu.sustech.cs307.exception.DBException;
import edu.sustech.cs307.logicalOperator.LogicalOperator;
import edu.sustech.cs307.meta.ColumnMeta;
import edu.sustech.cs307.optimizer.LogicalPlanner;
import edu.sustech.cs307.optimizer.PhysicalPlanner;
import edu.sustech.cs307.physicalOperator.PhysicalOperator;
import edu.sustech.cs307.system.DBManager;
import edu.sustech.cs307.tuple.Tuple;
import edu.sustech.cs307.value.Value;
import edu.sustech.cs307.value.ValueType;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.*;
import io.netty.util.CharsetUtil;
import org.pmw.tinylog.Logger;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

public class HttpServer {
    private final int port;
    private final DBManager dbManager;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private Channel serverChannel;
    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;

    public HttpServer(int port, DBManager dbManager) {
        this.port = port;
        this.dbManager = dbManager;
    }

    public void start() throws InterruptedException {
        bossGroup = new NioEventLoopGroup(1);
        workerGroup = new NioEventLoopGroup();

        try {
            ServerBootstrap bootstrap = new ServerBootstrap();
            bootstrap.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) {
                            ChannelPipeline pipeline = ch.pipeline();
                            pipeline.addLast(new HttpServerCodec());
                            pipeline.addLast(new HttpObjectAggregator(65536));
                            pipeline.addLast(new HttpServerHandler(dbManager, objectMapper));
                        }
                    })
                    .option(ChannelOption.SO_BACKLOG, 128)
                    .childOption(ChannelOption.SO_KEEPALIVE, true);

            ChannelFuture future = bootstrap.bind(port).sync();
            serverChannel = future.channel();
            Logger.info("HTTP Server started on port " + port);
        } catch (Exception e) {
            Logger.error("Failed to start HTTP server: " + e.getMessage());
            shutdown();
            throw e;
        }
    }

    public void shutdown() {
        if (serverChannel != null) {
            serverChannel.close();
        }
        if (workerGroup != null) {
            workerGroup.shutdownGracefully();
        }
        if (bossGroup != null) {
            bossGroup.shutdownGracefully();
        }
    }

    private static class HttpServerHandler extends SimpleChannelInboundHandler<FullHttpRequest> {
        private final DBManager dbManager;
        private final ObjectMapper objectMapper;

        public HttpServerHandler(DBManager dbManager, ObjectMapper objectMapper) {
            this.dbManager = dbManager;
            this.objectMapper = objectMapper;
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, FullHttpRequest request) {
            try {
                String sql = extractSqlFromRequest(request);
                Map<String, Object> response = new LinkedHashMap<>();

                if (sql == null || sql.trim().isEmpty()) {
                    response.put("status", "error");
                    response.put("message", "Missing 'sql' parameter");
                    sendJsonResponse(ctx, response, HttpResponseStatus.BAD_REQUEST);
                    return;
                }

                response = executeSql(sql);
                sendJsonResponse(ctx, response, HttpResponseStatus.OK);

            } catch (Exception e) {
                Map<String, Object> errorResponse = new LinkedHashMap<>();
                errorResponse.put("status", "error");
                errorResponse.put("message", "Server error: " + e.getMessage());
                sendJsonResponse(ctx, errorResponse, HttpResponseStatus.INTERNAL_SERVER_ERROR);
            }
        }

        private String extractSqlFromRequest(FullHttpRequest request) {
            try {
                if (request.method() == HttpMethod.GET) {
                    return parseQueryParams(request.uri()).get("sql");
                } else if (request.method() == HttpMethod.POST) {
                    String body = request.content().toString(CharsetUtil.UTF_8);
                    return parseFormData(body).get("sql");
                }
            } catch (Exception e) {
                Logger.error("Error extracting SQL: " + e.getMessage());
            }
            return null;
        }

        private Map<String, String> parseQueryParams(String uri) {
            Map<String, String> params = new HashMap<>();
            int questionIndex = uri.indexOf('?');
            if (questionIndex >= 0) {
                String queryString = uri.substring(questionIndex + 1);
                for (String pair : queryString.split("&")) {
                    String[] keyValue = pair.split("=", 2);
                    if (keyValue.length == 2) {
                        try {
                            String key = URLDecoder.decode(keyValue[0], StandardCharsets.UTF_8);
                            String value = URLDecoder.decode(keyValue[1], StandardCharsets.UTF_8);
                            params.put(key, value);
                        } catch (Exception e) {
                            Logger.error("Error parsing query param: " + e.getMessage());
                        }
                    }
                }
            }
            return params;
        }

        private Map<String, String> parseFormData(String body) {
            Map<String, String> params = new HashMap<>();
            for (String pair : body.split("&")) {
                String[] keyValue = pair.split("=", 2);
                if (keyValue.length == 2) {
                    try {
                        String key = URLDecoder.decode(keyValue[0], StandardCharsets.UTF_8);
                        String value = URLDecoder.decode(keyValue[1], StandardCharsets.UTF_8);
                        params.put(key, value);
                    } catch (Exception e) {
                        Logger.error("Error parsing form data: " + e.getMessage());
                    }
                }
            }
            return params;
        }

        private Map<String, Object> executeSql(String sql) {
            Map<String, Object> response = new LinkedHashMap<>();

            try {
                if (sql.equalsIgnoreCase("help;")) {
                    response.put("status", "help");
                    response.put("message",
                            "Available commands:\n- help; - Show this help\n- exit; - Exit (only in CLI)\n- Any SQL query");
                    return response;
                }

                LogicalOperator operator = LogicalPlanner.resolveAndPlan(dbManager, sql);
                if (operator == null) {
                    response.put("status", "error");
                    response.put("message", "Failed to parse SQL statement");
                    return response;
                }

                PhysicalOperator physicalOperator = PhysicalPlanner.generateOperator(dbManager, operator);
                if (physicalOperator == null) {
                    response.put("status", "error");
                    response.put("message", "No physical operator generated");
                    return response;
                }

                // Execute query and collect results
                physicalOperator.Begin();
                ArrayList<ColumnMeta> schema = physicalOperator.outputSchema();

                ArrayList<Map<String, Object>> rows = new ArrayList<>();

                // Process rows
                while (physicalOperator.hasNext()) {
                    physicalOperator.Next();
                    Tuple tuple = physicalOperator.Current();

                    // Add JSON row
                    Map<String, Object> row = new LinkedHashMap<>();
                    Value[] values = tuple != null ? tuple.getValues() : null;

                    for (int i = 0; i < schema.size(); i++) {
                        ColumnMeta cm = schema.get(i);
                        String columnName = cm.tableName == null ? cm.name : cm.tableName + "." + cm.name;
                        Object value = null;

                        if (values != null && i < values.length && values[i] != null && !values[i].isNull()) {
                            value = values[i].value;
                        }
                        if (values[i].type == ValueType.CHAR) {
                            value = values[i].toString().trim();
                        }
                        row.put(columnName, value);
                    }
                    rows.add(row);
                }

                physicalOperator.Close();
                dbManager.getBufferPool().FlushAllPages("");

                response.put("status", "success");
                response.put("message", "Query executed successfully");
                response.put("data", rows);
                response.put("rowCount", rows.size());

            } catch (DBException e) {
                response.put("status", "error");
                response.put("message", "Database error: " + e.getMessage());
            } catch (Exception e) {
                response.put("status", "error");
                response.put("message", "Unexpected error: " + e.getMessage());
            }

            return response;
        }

        private void sendJsonResponse(ChannelHandlerContext ctx, Map<String, Object> data, HttpResponseStatus status) {
            try {
                String json = objectMapper.writeValueAsString(data);
                FullHttpResponse response = new DefaultFullHttpResponse(
                        HttpVersion.HTTP_1_1, status, Unpooled.copiedBuffer(json, CharsetUtil.UTF_8));

                response.headers().set(HttpHeaderNames.CONTENT_TYPE, "application/json; charset=UTF-8");
                response.headers().set(HttpHeaderNames.CONTENT_LENGTH, response.content().readableBytes());
                response.headers().set(HttpHeaderNames.ACCESS_CONTROL_ALLOW_ORIGIN, "*");
                response.headers().set(HttpHeaderNames.ACCESS_CONTROL_ALLOW_METHODS, "GET, POST, OPTIONS");
                response.headers().set(HttpHeaderNames.ACCESS_CONTROL_ALLOW_HEADERS, "Content-Type");

                ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
            } catch (Exception e) {
                Logger.error("Error sending JSON response: " + e.getMessage());
                ctx.close();
            }
        }
    }
}