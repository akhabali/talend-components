package org.talend.components.jdbc;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.derby.drda.NetworkServerControl;
import org.apache.derby.jdbc.ClientDataSource;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;

import lombok.Data;

public class DerbyExtension implements BeforeAllCallback, AfterAllCallback, ParameterResolver {

    private NetworkServerControl serverControl;

    private ClientDataSource dataSource;

    private DerbyInfo derbyDb;

    @Override
    public void beforeAll(final ExtensionContext context) throws Exception {
        final WithDerby element =
                context.getElement().map(e -> e.getAnnotation(WithDerby.class)).orElseThrow(
                        () -> new IllegalArgumentException(
                                "No annotation @WithDerby on " + context.getRequiredTestClass()));

        final int port;
        if (element.port() == 0) {
            ServerSocket socket = new ServerSocket(0);
            port = socket.getLocalPort();
            socket.close();
        } else {
            port = element.port();
        }
        System.setProperty("derby.stream.error.file", element.logFile());
        final String dbName = Files.createTempDirectory("derby").toFile().getAbsolutePath() + "/" + element.dbName();
        final String url = "jdbc:derby://" + element.server() + ":" + port + "/" + dbName;
        serverControl = new NetworkServerControl(InetAddress.getByName(element.server()), port);
        serverControl.start(null);
        dataSource = new ClientDataSource();
        if (element.createDb()) {
            dataSource.setCreateDatabase("create");
        }
        dataSource.setDatabaseName(dbName);
        dataSource.setServerName(element.server());
        dataSource.setPortNumber(port);
        dataSource.setUser(element.user());
        if (!element.password().isEmpty()) {
            dataSource.setPassword(element.password());
        }
        try (Connection c = dataSource.getConnection()) {  //validate server
            if (!c.isValid(3)) {
                throw new IllegalStateException("Error while starting Derby server");
            }
            turnOnBuiltInUsers(c, element.password());

            if (!element.onStartSQLScript().isEmpty()) {
                execScript(element.onStartSQLScript(), c);
            }
        }

        derbyDb = new DerbyInfo(element.server(), port, dbName);
    }

    private void execScript(final String script, final Connection c) {
        try (InputStream is = this.getClass().getClassLoader().getResourceAsStream(script)) {
            new BufferedReader(new InputStreamReader(is)).lines()
                    .filter(l -> !l.isEmpty())
                    .forEach(query -> {
                        try (Statement statement = c.createStatement()) {
                            statement.executeUpdate(query);
                        } catch (SQLException e) {
                            throw new IllegalStateException(e);
                        }
                    });
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public void afterAll(final ExtensionContext context) throws Exception {
        final WithDerby element =
                context.getElement().map(e -> e.getAnnotation(WithDerby.class)).orElseThrow(
                        () -> new IllegalArgumentException(
                                "No annotation @WithDerby on " + context.getRequiredTestClass()));
        try {
            if (!element.onShutdownSQLScript().isEmpty()) {
                try (InputStream is = this.getClass()
                        .getClassLoader()
                        .getResourceAsStream(element.onShutdownSQLScript())) {
                    new BufferedReader(new InputStreamReader(is)).lines()
                            .filter(l -> !l.isEmpty())
                            .forEach(l -> {
                                try (Connection connection = dataSource.getConnection()) {
                                    try (Statement statement = connection.createStatement()) {
                                        statement.executeUpdate(l);
                                    }
                                } catch (SQLException e) {
                                    throw new IllegalStateException(e);
                                }
                            });
                }
            }
        } finally {
            if (serverControl != null) {
                serverControl.shutdown();
            }
        }
    }

    private void turnOnBuiltInUsers(final Connection conn, final String password) throws SQLException {
        try (Statement s = conn.createStatement()) {
            // Setting and Confirming requireAuthentication
            s.executeUpdate("CALL SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY(" +
                    "'derby.connection.requireAuthentication', 'true')");
            // Setting authentication scheme to Derby
            s.executeUpdate(
                    "CALL SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY(" + "'derby.authentication.provider', 'BUILTIN')");
            // Creating some sample users
            s.executeUpdate("CALL SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY(" + "'derby.user.sa', '" + password + "')");
            s.executeUpdate("CALL SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY(" + "'derby.user.guest', '" + password + "')");
            s.executeUpdate("CALL SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY(" + "'derby.user.admin', '" + password + "')");
            // Setting default connection mode to no access
            s.executeUpdate(
                    "CALL SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY("
                            + "'derby.database.defaultConnectionMode', 'noAccess')");

            // Defining read-write users
            s.executeUpdate(
                    "CALL SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY(" + "'derby.database.fullAccessUsers', 'sa,admin')");

            // Defining read-only users
            s.executeUpdate(
                    "CALL SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY(" + "'derby.database.readOnlyAccessUsers', 'guest')");

            s.executeUpdate(
                    "CALL SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY(" + "'derby.database.propertiesOnly', 'false')");
        }
    }

    @Override
    public boolean supportsParameter(final ParameterContext parameterContext, final ExtensionContext extensionContext)
            throws ParameterResolutionException {

        return DerbyInfo.class == parameterContext.getParameter().getType();
    }

    @Override
    public Object resolveParameter(final ParameterContext parameterContext, final ExtensionContext extensionContext)
            throws ParameterResolutionException {

        if (DerbyInfo.class == parameterContext.getParameter().getType()) {
            return derbyDb;
        }

        return null;
    }

    @Data
    public static class DerbyInfo {

        private final String server;

        private final int port;

        private final String dbName;
    }
}
