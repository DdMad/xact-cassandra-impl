/**
 * Created by ddmad on 9/10/16.
 */
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

import java.io.IOException;
import java.sql.Timestamp;


public class Test {

    private static final String SCHEMA_FILE_PATH = "/src/main/resources/setup-draft.cql";
    private static final String DATA_DIRECTORY = "/src/main/resources/D8-data/";
    private static final String XACT_DIRECTORY = "/src/main/resources/D8-xact-data/0.txt";

    public static void main(String[] args) {

        Cluster cluster;
        Session session;

        // Connect to the cluster
        cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
        session = cluster.connect();

        DatabaseBuilder builder = new DatabaseBuilder(System.getProperty("user.dir") + SCHEMA_FILE_PATH);
        try {
            builder.buildDatabase(session);
        } catch (IOException e) {
            System.out.println("Build database failed!");
            e.printStackTrace();
        }
        System.out.println("Build database successfully!");

        DataLoader loader = new DataLoader(System.getProperty("user.dir") + DATA_DIRECTORY);
        try {
            loader.loadData(session);
        } catch (IOException e) {
            System.out.println("Build database failed!");
            e.printStackTrace();
        }
        System.out.println("Load data successfully!");

        XactProcessor processor = new XactProcessor(System.getProperty("user.dir") + XACT_DIRECTORY);
        try {
            processor.processXact(session);
        } catch (IOException e) {
            e.printStackTrace();
        }


        cluster.close();
    }
}
