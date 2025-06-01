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

// 添加JSQLParser相关导入
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.parser.JSqlParser;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.DescribeStatement;
import net.sf.jsqlparser.statement.ExplainStatement;
import net.sf.jsqlparser.statement.show.ShowTablesStatement;
import net.sf.jsqlparser.statement.ShowColumnsStatement;

import java.io.StringReader;
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
    public Channel serverChannel;
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
        Logger.info("Shutting down HTTP server...");
        try {
            if (serverChannel != null) {
                serverChannel.close().sync();
            }
        } catch (InterruptedException e) {
            Logger.warn("Interrupted while closing server channel");
            Thread.currentThread().interrupt();
        }

        if (workerGroup != null) {
            try {
                workerGroup.shutdownGracefully().sync();
            } catch (InterruptedException e) {
                Logger.warn("Interrupted while shutting down worker group");
                Thread.currentThread().interrupt();
            }
        }

        if (bossGroup != null) {
            try {
                bossGroup.shutdownGracefully().sync();
            } catch (InterruptedException e) {
                Logger.warn("Interrupted while shutting down boss group");
                Thread.currentThread().interrupt();
            }
        }
        Logger.info("HTTP server shutdown completed");
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
                String trimmedSql = sql.trim();
                if (trimmedSql.isEmpty()) {
                    response.put("status", "error");
                    response.put("message", "Empty SQL statement");
                    return response;
                }
                String[] sqlStatements = splitSqlStatements(trimmedSql);

                if (sqlStatements.length == 0) {
                    response.put("status", "error");
                    response.put("message", "No valid SQL statements found");
                    return response;
                }

                if (sqlStatements.length == 1) {
                    return executeSingleSql(sqlStatements[0].trim());
                }

                ArrayList<Map<String, Object>> results = new ArrayList<>();
                int successCount = 0;
                int errorCount = 0;
                StringBuilder errorMessages = new StringBuilder();

                for (int i = 0; i < sqlStatements.length; i++) {
                    String singleSql = sqlStatements[i].trim();
                    if (singleSql.isEmpty()) {
                        continue;
                    }

                    try {
                        Map<String, Object> singleResult = executeSingleSql(singleSql);
                        singleResult.put("statementIndex", i + 1);
                        singleResult.put("sql", singleSql);
                        results.add(singleResult);

                        if ("success".equals(singleResult.get("status")) || "help".equals(singleResult.get("status"))) {
                            successCount++;
                        } else {
                            errorCount++;
                            if (errorMessages.length() > 0) {
                                errorMessages.append("; ");
                            }
                            errorMessages.append("Statement ").append(i + 1).append(": ")
                                    .append(singleResult.get("message"));
                        }
                    } catch (Exception e) {
                        errorCount++;
                        Map<String, Object> errorResult = new LinkedHashMap<>();
                        errorResult.put("status", "error");
                        errorResult.put("message", "Error executing statement: " + e.getMessage());
                        errorResult.put("statementIndex", i + 1);
                        errorResult.put("sql", singleSql);
                        results.add(errorResult);

                        if (errorMessages.length() > 0) {
                            errorMessages.append("; ");
                        }
                        errorMessages.append("Statement ").append(i + 1).append(": ").append(e.getMessage());
                    }
                }

                response.put("status", errorCount == 0 ? "success" : (successCount == 0 ? "error" : "partial"));
                response.put("message", String.format("Executed %d statements: %d successful, %d failed",
                        sqlStatements.length, successCount, errorCount));
                response.put("totalStatements", sqlStatements.length);
                response.put("successCount", successCount);
                response.put("errorCount", errorCount);
                response.put("results", results);

                if (errorCount > 0) {
                    response.put("errors", errorMessages.toString());
                }

            } catch (Exception e) {
                response.put("status", "error");
                response.put("message", "Unexpected error during SQL execution: " + e.getMessage());
            }

            return response;
        }

        /**
         * 分割SQL语句字符串为多个独立的SQL语句
         */
        private String[] splitSqlStatements(String sql) {
            ArrayList<String> statements = new ArrayList<>();
            StringBuilder currentStatement = new StringBuilder();
            boolean inSingleQuote = false;
            boolean inDoubleQuote = false;
            boolean inComment = false;

            for (int i = 0; i < sql.length(); i++) {
                char c = sql.charAt(i);
                char nextChar = (i + 1 < sql.length()) ? sql.charAt(i + 1) : '\0';

                // 处理字符串引号
                if (c == '\'' && !inDoubleQuote) {
                    inSingleQuote = !inSingleQuote;
                    currentStatement.append(c);
                    continue;
                }

                if (c == '"' && !inSingleQuote) {
                    inDoubleQuote = !inDoubleQuote;
                    currentStatement.append(c);
                    continue;
                }

                // 如果在引号内，直接添加字符
                if (inSingleQuote || inDoubleQuote) {
                    currentStatement.append(c);
                    continue;
                }

                // 处理分号分隔符
                if (c == ';') {
                    currentStatement.append(c);
                    String statement = currentStatement.toString().trim();
                    if (!statement.isEmpty()) {
                        statements.add(statement);
                    }
                    currentStatement = new StringBuilder();
                } else {
                    currentStatement.append(c);
                }
            }

            // 添加最后一个语句（如果没有以分号结尾）
            String lastStatement = currentStatement.toString().trim();
            if (!lastStatement.isEmpty()) {
                statements.add(lastStatement);
            }

            return statements.toArray(new String[0]);
        }

        /**
         * 执行单条SQL语句 - 使用JSQLParser进行解析
         */
        private Map<String, Object> executeSingleSql(String sql) {
            // 直接使用JSQLParser进行解析和执行
            return parseAndExecuteSql(sql);
        }

        /**
         * 使用JSQLParser解析和识别SQL语句类型
         */
        private Map<String, Object> parseAndExecuteSql(String sql) {
            Map<String, Object> response = new LinkedHashMap<>();

            try {
                JSqlParser parser = new CCJSqlParserManager();
                Statement stmt = parser.parse(new StringReader(sql));

                // 使用JSQLParser识别语句类型并处理
                if (stmt instanceof ShowTablesStatement) {
                    return handleShowTables();
                } else if (stmt instanceof DescribeStatement) {
                    DescribeStatement descStmt = (DescribeStatement) stmt;
                    return handleDescribeTable(descStmt.getTable().getName());
                } else if (stmt instanceof ShowColumnsStatement) {
                    ShowColumnsStatement showColsStmt = (ShowColumnsStatement) stmt;
                    return handleDescribeTable(showColsStmt.getTableName());
                } else if (stmt instanceof ExplainStatement) {
                    ExplainStatement explainStmt = (ExplainStatement) stmt;
                    return handleExplainQuery(explainStmt.getStatement().toString());
                } else {
                    // 对于其他SQL语句，使用现有的LogicalPlanner处理
                    return executeNormalSql(sql);
                }

            } catch (JSQLParserException e) {
                // 如果JSQLParser无法解析，尝试手动处理一些特殊情况
                String upperSql = sql.trim().toUpperCase();

                if (upperSql.equals("HELP;") || upperSql.equals("HELP")) {
                    return handleHelp();
                } else if (upperSql.equals("SHOW TABLES;") || upperSql.equals("SHOW TABLES")) {
                    return handleShowTables();
                } else if (upperSql.startsWith("DESC ") || upperSql.startsWith("DESCRIBE ")) {
                    String tableName = extractTableNameFromDesc(sql);
                    return handleDescribeTable(tableName);
                } else if (upperSql.startsWith("EXPLAIN ")) {
                    String queryToExplain = sql.trim().substring(8).trim();
                    return handleExplainQuery(queryToExplain);
                } else {
                    response.put("status", "error");
                    response.put("message", "Failed to parse SQL: " + e.getMessage());
                    return response;
                }
            } catch (Exception e) {
                response.put("status", "error");
                response.put("message", "Error processing SQL: " + e.getMessage());
                return response;
            }
        }

        /**
         * 处理HELP命令
         */
        private Map<String, Object> handleHelp() {
            Map<String, Object> response = new LinkedHashMap<>();
            response.put("status", "help");
            response.put("message",
                    "Available commands:\n" +
                            "- HELP; - Show this help\n" +
                            "- SHOW TABLES; - List all tables\n" +
                            "- DESC <table_name>; - Describe table structure\n" +
                            "- EXPLAIN <query>; - Show query execution plan\n" +
                            "- SELECT/INSERT/UPDATE/DELETE statements\n" +
                            "- CREATE/DROP TABLE statements");
            return response;
        }

        /**
         * 处理SHOW TABLES命令
         */
        private Map<String, Object> handleShowTables() {
            Map<String, Object> response = new LinkedHashMap<>();
            try {
                java.util.List<String> tableNames = dbManager.getTableNamesList();
                ArrayList<Map<String, Object>> rows = new ArrayList<>();

                for (String tableName : tableNames) {
                    Map<String, Object> row = new LinkedHashMap<>();
                    row.put("Tables", tableName);
                    rows.add(row);
                }

                response.put("status", "success");
                response.put("message", "Tables listed successfully");
                response.put("data", rows);
                response.put("rowCount", rows.size());
            } catch (Exception e) {
                response.put("status", "error");
                response.put("message", "Error listing tables: " + e.getMessage());
            }
            return response;
        }

        /**
         * 处理DESC/DESCRIBE命令
         */
        private Map<String, Object> handleDescribeTable(String tableName) {
            Map<String, Object> response = new LinkedHashMap<>();
            try {
                if (tableName == null || tableName.trim().isEmpty()) {
                    response.put("status", "error");
                    response.put("message", "Table name is required for DESC command");
                    return response;
                }

                // 清理表名（移除分号等）
                tableName = tableName.replace(";", "").trim();

                java.util.List<Map<String, Object>> columns = dbManager.getTableColumns(tableName);

                response.put("status", "success");
                response.put("message", "Table structure retrieved successfully");
                response.put("data", columns);
                response.put("rowCount", columns.size());
            } catch (DBException e) {
                response.put("status", "error");
                response.put("message", "Database error: " + e.getMessage());
            } catch (Exception e) {
                response.put("status", "error");
                response.put("message", "Error describing table: " + e.getMessage());
            }
            return response;
        }

        /**
         * 处理EXPLAIN命令
         */
        private Map<String, Object> handleExplainQuery(String queryToExplain) {
            Map<String, Object> response = new LinkedHashMap<>();
            try {
                LogicalOperator logicalOp = LogicalPlanner.resolveAndPlan(dbManager, queryToExplain);
                if (logicalOp == null) {
                    response.put("status", "error");
                    response.put("message", "Failed to parse query for explanation");
                    return response;
                }

                PhysicalOperator physicalOp = PhysicalPlanner.generateOperator(dbManager, logicalOp);
                if (physicalOp == null) {
                    response.put("status", "error");
                    response.put("message", "Failed to generate physical plan");
                    return response;
                }

                // 创建简单的执行计划说明
                ArrayList<Map<String, Object>> planRows = new ArrayList<>();
                Map<String, Object> planRow = new LinkedHashMap<>();
                planRow.put("Operation", physicalOp.getClass().getSimpleName());
                planRow.put("Description", "Physical operator: " + physicalOp.getClass().getSimpleName());

                // 获取输出模式信息
                ArrayList<ColumnMeta> schema = physicalOp.outputSchema();
                StringBuilder schemaDesc = new StringBuilder();
                for (int i = 0; i < schema.size(); i++) {
                    if (i > 0)
                        schemaDesc.append(", ");
                    ColumnMeta cm = schema.get(i);
                    schemaDesc.append(cm.name).append("(").append(cm.type).append(")");
                }
                planRow.put("Output_Schema", schemaDesc.toString());
                planRows.add(planRow);

                response.put("status", "success");
                response.put("message", "Query plan generated successfully");
                response.put("data", planRows);
                response.put("rowCount", planRows.size());
            } catch (Exception e) {
                response.put("status", "error");
                response.put("message", "Error explaining query: " + e.getMessage());
            }
            return response;
        }

        /**
         * 执行普通SQL语句（SELECT, INSERT, UPDATE, DELETE, CREATE, DROP等）
         */
        private Map<String, Object> executeNormalSql(String sql) {
            Map<String, Object> response = new LinkedHashMap<>();

            // 开始计时
            long startTime = System.nanoTime();

            try {
                LogicalOperator operator = LogicalPlanner.resolveAndPlan(dbManager, sql);
                if (operator == null) {
                    // 计算执行时间
                    long endTime = System.nanoTime();
                    double executionTimeMs = (endTime - startTime) / 1_000_000.0;

                    response.put("status", "success");
                    response.put("message", "Statement executed successfully");
                    response.put("data", new ArrayList<>());
                    response.put("rowCount", 0);
                    response.put("executionTimeMs", executionTimeMs);
                    return response;
                }

                PhysicalOperator physicalOperator = PhysicalPlanner.generateOperator(dbManager, operator);
                if (physicalOperator == null) {
                    // 计算执行时间
                    long endTime = System.nanoTime();
                    double executionTimeMs = (endTime - startTime) / 1_000_000.0;

                    response.put("status", "error");
                    response.put("message", "No physical operator generated");
                    response.put("executionTimeMs", executionTimeMs);
                    return response;
                }

                // Execute query and collect results
                physicalOperator.Begin();
                ArrayList<ColumnMeta> schema = physicalOperator.outputSchema();
                ArrayList<Map<String, Object>> rows = new ArrayList<>();

                // 对于DDL语句（CREATE, DROP等），可能没有返回结果
                if (schema.isEmpty()) {
                    // 计算执行时间
                    long endTime = System.nanoTime();
                    double executionTimeMs = (endTime - startTime) / 1_000_000.0;

                    response.put("status", "success");
                    response.put("message", "Statement executed successfully");
                    response.put("data", new ArrayList<>());
                    response.put("rowCount", 0);
                    response.put("executionTimeMs", executionTimeMs);
                } else {
                    // DML语句或查询语句，处理返回的数据
                    while (physicalOperator.hasNext()) {
                        physicalOperator.Next();
                        Tuple tuple = physicalOperator.Current();

                        Map<String, Object> row = new LinkedHashMap<>();
                        Value[] values = tuple != null ? tuple.getValues() : null;

                        for (int i = 0; i < schema.size(); i++) {
                            ColumnMeta cm = schema.get(i);
                            String columnName = cm.tableName == null ? cm.name : cm.tableName + "." + cm.name;
                            Object value = null;

                            if (values != null && i < values.length && values[i] != null && !values[i].isNull()) {
                                if (values[i].type == ValueType.CHAR) {
                                    value = values[i].toString().trim();
                                } else {
                                    value = values[i].value;
                                }
                            }
                            row.put(columnName, value);
                        }
                        rows.add(row);
                    }

                    // 计算执行时间
                    long endTime = System.nanoTime();
                    double executionTimeMs = (endTime - startTime) / 1_000_000.0;

                    response.put("status", "success");
                    response.put("message", "Query executed successfully");
                    response.put("data", rows);
                    response.put("rowCount", rows.size());
                    response.put("executionTimeMs", executionTimeMs);
                }

                physicalOperator.Close();
                dbManager.getBufferPool().FlushAllPages("");

            } catch (DBException e) {
                // 计算执行时间
                long endTime = System.nanoTime();
                double executionTimeMs = (endTime - startTime) / 1_000_000.0;

                response.put("status", "error");
                response.put("message", "Database error: " + e.getMessage());
                response.put("executionTimeMs", executionTimeMs);
            } catch (Exception e) {
                // 计算执行时间
                long endTime = System.nanoTime();
                double executionTimeMs = (endTime - startTime) / 1_000_000.0;

                response.put("status", "error");
                response.put("message", "Unexpected error: " + e.getMessage());
                response.put("executionTimeMs", executionTimeMs);
            }

            return response;
        }

        /**
         * 从DESC语句中提取表名（备用方法，当JSQLParser无法解析时使用）
         */
        private String extractTableNameFromDesc(String sql) {
            String trimmedSql = sql.trim();
            String upperSql = trimmedSql.toUpperCase();

            if (upperSql.startsWith("DESC ")) {
                return trimmedSql.substring(5).trim().replace(";", "");
            } else if (upperSql.startsWith("DESCRIBE ")) {
                return trimmedSql.substring(9).trim().replace(";", "");
            }
            return "";
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