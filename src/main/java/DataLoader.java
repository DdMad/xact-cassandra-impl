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

    public void loadData(Session session) throws IOException {
        for (final File dataFile : new File(dataFileDir).listFiles()) {
            br = new BufferedReader(new FileReader(dataFile));
            String name = dataFile.getName();

            if (name.equals(DATA_CUST_HIST)) {
                loadCustHistData(session, br);
            } else if (name.equals(DATA_CUSTOMER)) {
                loadCustomerData(session, br);
            } else if (name.equals(DATA_DISTRICT)) {
                loadDistrictData(session, br);
            } else if (name.equals(DATA_ITEM)) {
                loadItemData(session, br);
            } else if (name.equals(DATA_NEW_ORDER)) {
                loadNewOrderData(session, br);
            } else if (name.equals(DATA_ORDER)) {
                loadOrderData(session, br);
            } else if (name.equals(DATA_ORDER_LINE)) {
                loadOrderLineData(session, br);
            } else if (name.equals(DATA_STOCK)) {
                loadStockData(session, br);
            } else if (name.equals(DATA_WAREHOUSE)) {
                loadWarehouseData(session, br);
            } else {
                logger.warn("Wrong data file!");
            }
        }
    }

    private void loadCustHistData(Session session, BufferedReader br) throws IOException {
        String dataLine = br.readLine();
        while (dataLine != null) {
            String[] data = dataLine.split(",");

        }
    }

    private void loadCustomerData(Session session, BufferedReader br) throws IOException {
        String dataLine = br.readLine();
        while (dataLine != null) {
            String[] data = dataLine.split(",");

        }
    }

    private void loadDistrictData(Session session, BufferedReader br) throws IOException {
        String dataLine = br.readLine();
        while (dataLine != null) {
            String[] data = dataLine.split(",");

        }
    }

    private void loadItemData(Session session, BufferedReader br) throws IOException {
        String dataLine = br.readLine();
        while (dataLine != null) {
            String[] data = dataLine.split(",");

        }
    }

    private void loadNewOrderData(Session session, BufferedReader br) throws IOException {
        String dataLine = br.readLine();
        while (dataLine != null) {
            String[] data = dataLine.split(",");

        }
    }

    private void loadOrderData(Session session, BufferedReader br) throws IOException {
        String dataLine = br.readLine();
        while (dataLine != null) {
            String[] data = dataLine.split(",");

        }
    }

    private void loadOrderLineData(Session session, BufferedReader br) throws IOException {
        String dataLine = br.readLine();
        while (dataLine != null) {
            String[] data = dataLine.split(",");

        }
    }

    private void loadStockData(Session session, BufferedReader br) throws IOException {
        String dataLine = br.readLine();
        while (dataLine != null) {
            String[] data = dataLine.split(",");

        }
    }

    private void loadWarehouseData(Session session, BufferedReader br) throws IOException {
        String dataLine = br.readLine();
        while (dataLine != null) {
            String[] data = dataLine.split(",");

        }
    }
}
