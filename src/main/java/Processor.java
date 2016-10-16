import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;

/**
 * Created by ddmad on 16/10/16.
 */
public class Processor {
    private static final String D8_XACT_DIRECTORY = "/D8-xact-data/";
    private static final String D40_XACT_DIRECTORY = "/D40-xact-data/";

    public static void main(String[] args) {

        Cluster cluster;
        Session session;

        // For compute performance
        long count = 0;
        long start = System.currentTimeMillis();

        // Connect to the cluster
        cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
        session = cluster.connect();

        long[] xactCount = new long[7];
        long[] xactTime = new long[7];

        if (args[0].toLowerCase().equals("d8")) {
            session.execute("use d8");

            XactProcessor processor = new XactProcessor(System.getProperty("user.dir") + D8_XACT_DIRECTORY + args[1]);
            try {
                count = processor.processXact(session, xactCount, xactTime);
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else if (args[0].toLowerCase().equals("d40")) {
            session.execute("use d40");

            XactProcessor processor = new XactProcessor(System.getProperty("user.dir") + D40_XACT_DIRECTORY + args[1]);
            try {
                count = processor.processXact(session, xactCount, xactTime);
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else {
            System.out.println("Argument Error");
        }

        cluster.close();

        long end = System.currentTimeMillis();
        long duration = end - start;

        if (args[0].toLowerCase().equals("d8")) {
            try {
                System.setErr(new PrintStream(System.getProperty("user.dir") + D8_XACT_DIRECTORY + "result-d8-" + args[1] + ".out"));
                System.err.println("Total transactions processed: " + count);
                System.err.println("Total time (second) used: " + duration);
                System.err.println("Transaction throughput (xact per second): " + ((double) count)/((double) duration) * 1000);

                System.out.println("Total transactions processed: " + count);
                System.out.println("Total time (second) used: " + duration);
                System.out.println("Transaction throughput (xact per second): " + ((double) count)/((double) duration) * 1000);
                for (int i = 0; i < 7; i++) {
                    System.out.println((i + 1) + " transactions processed: " + xactCount[i]);
                    System.out.println("Subtotal time (second) used: " + xactTime[i]);
                    System.out.println((i + 1) + " transaction throughput (xact per second): " + ((double) xactCount[i])/((double) xactTime[i]) * 1000);
                }
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
        } else if (args[0].toLowerCase().equals("d40")) {
            try {
                System.setErr(new PrintStream(System.getProperty("user.dir") + D40_XACT_DIRECTORY + "result-d40-" + args[1] + ".out"));
                System.err.println("Total transactions processed: " + count);
                System.err.println("Total time (second) used: " + duration);
                System.err.println("Transaction throughput (xact per second): " + ((double) count)/((double) duration));

                System.out.println("Total transactions processed: " + count);
                System.out.println("Total time (second) used: " + duration);
                System.out.println("Transaction throughput (xact per second): " + ((double) count)/((double) duration) * 1000);
                for (int i = 0; i < 7; i++) {
                    System.out.println((i + 1) + " transactions processed: " + xactCount[i]);
                    System.out.println("Subtotal time (second) used: " + xactTime[i]);
                    System.out.println((i + 1) + " transaction throughput (xact per second): " + ((double) xactCount[i])/((double) xactTime[i]) * 1000);
                }
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
        } else {
            System.out.println("Error");
        }
    }
}
