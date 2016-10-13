import com.datastax.driver.core.Session;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import java.io.*;
import java.util.Properties;

/**
 * Created by ddmad on 13/10/16.
 */
public class XactProcessor {
    private String xactFileDir;
    private BufferedReader br;

    private Logger logger;

    public XactProcessor(String dir) {
        logger = Logger.getLogger(DatabaseBuilder.class.getName());
        String log4JPropertyFile = System.getProperty("user.dir") + "/log4j.properties";
        Properties p = new Properties();
        try {
            p.load(new FileInputStream(log4JPropertyFile));
            PropertyConfigurator.configure(p);
            logger.info("Log file is configured!");
        } catch (IOException e) {
            logger.error("Log file is not configured!");
        }

        xactFileDir = dir;
    }

    public void processXact(Session session) throws IOException {
        br = new BufferedReader(new FileReader(xactFileDir));
        String dataLine = br.readLine();
        while (dataLine != null) {
            String[] data = dataLine.split(",");
            String type = data[0];
            if (type.equals("N")) {
                processNewOrderXact(session, br);
            } else if (type.equals("P")) {
                processPaymentXact(session, br);
            } else if (type.equals("D")) {
                processDeliveryXact(session, br);
            } else if (type.equals("O")) {
                processOrderStatusXact(session, br);
            } else if (type.equals("S")) {
                processStockLevelXact(session, br);
            } else if (type.equals("I")) {
                processPopularItemXact(session, br);
            } else if (type.equals("T")) {
                processTopBalanceXact(session, br);
            } else {
                logger.warn("Wrong transaction type!");
            }
        }
    }

    private void processNewOrderXact(Session session, BufferedReader br) {

    }

    private void processPaymentXact(Session session, BufferedReader br) {

    }

    private void processDeliveryXact(Session session, BufferedReader br) {

    }

    private void processOrderStatusXact(Session session, BufferedReader br) {

    }

    private void processStockLevelXact(Session session, BufferedReader br) {

    }

    private void processPopularItemXact(Session session, BufferedReader br) {

    }

    private void processTopBalanceXact(Session session, BufferedReader br) {

    }
}
