package Database;

import SQLQueries.SQLQueries;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.logging.Logger;

public class DataBaseImpl implements DataBase {

    private static final Logger log;

    static {
        log = Logger.getLogger(DataBaseImpl.class.getName());
    }

    private final SQLQueries sqlQueries;
    private Connection connection;
    private Statement statement;

    public DataBaseImpl() {
        sqlQueries = new SQLQueries();
        Properties property = new Properties();
        try (InputStream in = Files.newInputStream(Paths.get("src/main/resources/property.properties"))) {
            property.load(in);
        } catch (IOException ex) {
            log.severe(ex.getMessage());
        }

        String url = property.getProperty("url");
        String user = property.getProperty("user");
        String password = property.getProperty("password");

        try {
            Class.forName("com.mysql.cj.jdbc.Driver").getDeclaredConstructor().newInstance();
            connection = DriverManager.getConnection(url, user, password);
            connection.setAutoCommit(false);
            this.statement = connection.createStatement();
            log.info("connection success");
        } catch (Exception ex) {
            log.severe(ex.getMessage());
        }

    }

    @Override
    public void createTable() {
        try {
            sqlQueries.sqlQueriesCreate.forEach((d) -> {
                try {
                    statement.execute(d);
                } catch (SQLException ex) {
                    log.severe(ex.getMessage());
                }
            });
            connection.commit();
            log.info("Data base created");
        } catch (SQLException ex) {
            log.severe(ex.getMessage());
        }
    }


    @Override
    public void write() {
        try {
            statement.execute(sqlQueries.fillingComposer());
            log.info("Composer filled");
            statement.execute(sqlQueries.fillingTextAuthor());
            log.info("Text author filled");
            statement.execute(sqlQueries.fillingMusicalProducer());
            log.info("Musical Producer filled");
            statement.execute(sqlQueries.fillingLabel());
            log.info("Label filled");
            statement.execute(sqlQueries.fillingGenre());
            log.info("Genre filled");
            statement.execute(sqlQueries.fillingGroupName());
            log.info("Group name filled");
            statement.execute(sqlQueries.fillingPerformer());
            log.info("Performer filled");
            statement.execute(sqlQueries.fillingComposition());
            log.info("Composition filled");
            statement.execute(sqlQueries.fillingCP());
            log.info("cp filled");
            statement.execute(sqlQueries.fillingAlbum());
            log.info("Album filled");
            statement.execute(sqlQueries.fillingMusicalComposition());
            log.info("Musical composition filled");
            statement.execute(sqlQueries.fillingGM());
            log.info("gm filled");
            connection.commit();
        } catch (SQLException ex) {
            log.severe(ex.getMessage());
        }
    }

    @Override
    public void close() {
        try {
            statement.close();
            connection.close();
        } catch (SQLException ex) {
            log.severe(ex.getMessage());
        }
    }
}
