import com.datastax.driver.core.Session;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import java.io.*;
import java.math.BigDecimal;
import java.text.DecimalFormat;
import java.util.Properties;

/**
 * Created by ddmad on 12/10/16.
 */
public class DataLoader {
    private static final String DATA_CUSTOMER = "customer.csv";
    private static final String DATA_DISTRICT = "district.csv";
    private static final String DATA_ITEM = "item.csv";
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

            if (name.equals(DATA_CUSTOMER)) {
                loadCustomerData(session, br);
            } else if (name.equals(DATA_DISTRICT)) {
                loadDistrictData(session, br);
            } else if (name.equals(DATA_ITEM)) {
                loadItemData(session, br);
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

    private void loadCustomerData(Session session, BufferedReader br) throws IOException {
        String dataLine = br.readLine();
        while (dataLine != null) {
            String[] data = dataLine.split(",");
            String cCreditLim = new DecimalFormat("0.00").format(new BigDecimal(data[14]));
            String cDiscount = new DecimalFormat("0.0000").format(new BigDecimal(data[15]));
            String cBalance = new DecimalFormat("0.00").format(new BigDecimal(data[16]));
            String query = String.format("INSERT INTO customer (W_ID, D_ID, C_ID, C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_DELIVERY_CNT) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)", data[0], data[1], data[2], cBalance, data[17], data[18], data[19]);
            String queryConstant = String.format("INSERT INTO customer_constant_data (W_ID, D_ID, C_ID, C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, C_CITY, C_STATE, C_ZIP, C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7], data[8], data[9], data[10], data[11], data[12], data[13], cCreditLim, cDiscount);
            String queryUnused = String.format("INSERT INTO customer_unused_data (W_ID, D_ID, C_ID, C_DATA) VALUES (%s, %s, %s, %s)", data[0], data[1], data[2], data[20]);
            session.execute(query);
            session.execute(queryConstant);
            session.execute(queryUnused);
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
            String w_tax = new DecimalFormat("0.0000").format(new BigDecimal(data[7]));
            String w_ytd = new DecimalFormat("0.00").format(new BigDecimal(data[8]));
            String query = String.format("INSERT INTO warehouse (W_ID, W_NAME, W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP, W_TAX, W_YTD) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)", data[0], data[1], data[2], data[3], data[4], data[5], data[6], w_tax, w_ytd);
            session.execute(query);
        }
    }
}
