import com.datastax.driver.core.Session;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import java.io.*;
import java.util.Properties;

/**
 * Created by ddmad on 12/10/16.
 */
public class DataLoader {
    private static final String DATA_CUST_HIST = "cust-hist.csv";
    private static final String DATA_CUSTOMER = "customer.csv";
    private static final String DATA_DISTRICT = "district.csv";
    private static final String DATA_ITEM = "item.csv";
    private static final String DATA_NEW_ORDER = "new-order.csv";
    private static final String DATA_ORDER_LINE = "order-line.csv";
    private static final String DATA_ORDER = "order.csv";
    private static final String DATA_STOCK = "stock.csv";
    private static final String DATA_WAREHOUSE = "warehouse.csv";

    private String dataFileDir;
    private BufferedReader br;

    private Logger logger;

    public DataLoader(String dir) {
        logger = Logger.getLogger(DbBuilder.class.getName());
        String log4JPropertyFile = System.getProperty("user.dir") + "/log4j.properties";
        Properties p = new Properties();
        try {
            p.load(new FileInputStream(log4JPropertyFile));
            PropertyConfigurator.configure(p);
            logger.info("Log file is configured!");
        } catch (IOException e) {
            logger.error("Log file is not configured!");
        }

        dataFileDir = dir;
    }

    public void loadData(Session session) throws FileNotFoundException {
        for (final File dataFile : new File(dataFileDir).listFiles()) {
            br = new BufferedReader(new FileReader(dataFile));
            String name = dataFile.getName();

            if (name.equals(DATA_CUST_HIST)) {

            } else if (name.equals(DATA_CUSTOMER)) {

            } else if (name.equals(DATA_DISTRICT)) {

            } else if (name.equals(DATA_ITEM)) {

            } else if (name.equals(DATA_NEW_ORDER)) {

            } else if (name.equals(DATA_ORDER)) {

            } else if (name.equals(DATA_ORDER_LINE)) {

            } else if (name.equals(DATA_STOCK)) {

            } else if (name.equals(DATA_WAREHOUSE)) {

            } else {
                logger.warn("Wrong data file!");
            }
        }
    }
}
