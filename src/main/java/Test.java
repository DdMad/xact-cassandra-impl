/**
 * Created by ddmad on 9/10/16.
 */
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

import java.io.IOException;


public class Test {

    private static final String SCHEMA_FILE_PATH = "/src/main/resources/setup.cql";
    private static final String DATA_DIRECTORY = "/src/main/resources/D8-data/";

    public static void main(String[] args) {

        Cluster cluster;
        Session session;

        // Connect to the cluster and keyspace "demo"
        cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
        session = cluster.connect();

        System.out.println();

        DbBuilder builder = new DbBuilder(System.getProperty("user.dir") + SCHEMA_FILE_PATH);
        try {
            builder.buildDatabase(session);
        } catch (IOException e) {
            e.printStackTrace();
        }

        cluster.close();
    }
}
