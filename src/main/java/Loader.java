/**
 * Created by ddmad on 9/10/16.
 */
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

import java.io.IOException;


public class Loader {

    private static final String D8_SCHEMA_FILE_PATH = "/resources/setup-d8.cql";
    private static final String D40_SCHEMA_FILE_PATH = "/resources/setup-d40.cql";
    private static final String D8_DATA_DIRECTORY = "/resources/D8-data/";
    private static final String D40_DATA_DIRECTORY = "/resources/D40-data/";


    public static void main(String[] args) {

        Cluster cluster;
        Session session;

        // Connect to the cluster
        cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
        session = cluster.connect();

        if (args[0].toLowerCase().equals("d8")) {
            DatabaseBuilder builder = new DatabaseBuilder(System.getProperty("user.dir") + D8_SCHEMA_FILE_PATH);
            try {
                builder.buildDatabase(session);
            } catch (IOException e) {
                System.out.println("Build D8 database failed!");
                e.printStackTrace();
            }
            System.out.println("Build D8 database successfully!");

            DataLoader loader = new DataLoader(System.getProperty("user.dir") + D8_DATA_DIRECTORY);
            try {
                loader.loadData(session);
            } catch (IOException e) {
                System.out.println("Load D8 database failed!");
                e.printStackTrace();
            }
            System.out.println("Load D8 data successfully!");
        } else if (args[0].toLowerCase().equals("d40")) {
            DatabaseBuilder builder = new DatabaseBuilder(System.getProperty("user.dir") + D40_SCHEMA_FILE_PATH);
            try {
                builder.buildDatabase(session);
            } catch (IOException e) {
                System.out.println("Build D40 database failed!");
                e.printStackTrace();
            }
            System.out.println("Build D40 database successfully!");

            DataLoader loader = new DataLoader(System.getProperty("user.dir") + D40_DATA_DIRECTORY);
            try {
                loader.loadData(session);
            } catch (IOException e) {
                System.out.println("Build D40 database failed!");
                e.printStackTrace();
            }
            System.out.println("Load D40 data successfully!");
        } else {
            System.out.println("Wrong data name!");
            return;
        }

        cluster.close();
    }
}
