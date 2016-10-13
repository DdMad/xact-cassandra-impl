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

        dataFileDir = dir;
    }

    public void loadData(Session session) throws IOException {
        for (final File dataFile : new File(dataFileDir).listFiles()) {
            br = new BufferedReader(new FileReader(dataFile));
            String name = dataFile.getName();

            logger.info("Process file: " + name);

            if (name.equals(DATA_CUSTOMER)) {
                loadCustomerData(session);
            } else if (name.equals(DATA_DISTRICT)) {
                loadDistrictData(session);
            } else if (name.equals(DATA_ITEM)) {
                loadItemData(session);
            } else if (name.equals(DATA_ORDER)) {
                loadOrderData(session);
            } else if (name.equals(DATA_ORDER_LINE)) {
                loadOrderLineData(session);
            } else if (name.equals(DATA_STOCK)) {
                loadStockData(session);
            } else if (name.equals(DATA_WAREHOUSE)) {
                loadWarehouseData(session);
            } else {
                logger.warn("Wrong data file for " + name + "!");
            }
        }
    }

    private void loadCustomerData(Session session) throws IOException {
        String dataLine = br.readLine();
        while (dataLine != null) {
            String[] data = dataLine.split(",");
            String cCreditLim = new DecimalFormat("0.00").format(new BigDecimal(data[14]));
            String cDiscount = new DecimalFormat("0.0000").format(new BigDecimal(data[15]));
            String cBalance = new DecimalFormat("0.00").format(new BigDecimal(data[16]));
            String query = String.format("INSERT INTO customer (W_ID, D_ID, C_ID, C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_DELIVERY_CNT) VALUES (%s, %s, %s, %s, %s, %s, %s)", data[0], data[1], data[2], cBalance, data[17], data[18], data[19]);
            String queryConstant = String.format("INSERT INTO customer_constant_data (W_ID, D_ID, C_ID, C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, C_CITY, C_STATE, C_ZIP, C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT) VALUES (%s, %s, %s, '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', %s, %s)", data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7], data[8], data[9], data[10], data[11], data[12], data[13], cCreditLim, cDiscount);
            String queryUnused = String.format("INSERT INTO customer_unused_data (W_ID, D_ID, C_ID, C_DATA) VALUES (%s, %s, %s, '%s')", data[0], data[1], data[2], data[20]);
            session.execute(query);
            session.execute(queryConstant);
            session.execute(queryUnused);

//            logger.info("Finish processing row: " + dataLine);
            dataLine = br.readLine();
        }

        logger.info("Complete loading custom column family!");
    }

    private void loadDistrictData(Session session) throws IOException {
        String dataLine = br.readLine();
        while (dataLine != null) {
            String[] data = dataLine.split(",");
            String cTax = new DecimalFormat("0.0000").format(new BigDecimal(data[8]));
            String cYtd = new DecimalFormat("0.00").format(new BigDecimal(data[9]));
            String query = String.format("INSERT INTO district (W_ID, D_ID, D_NAME, D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP, D_TAX, D_YTD) VALUES (%s, %s, '%s', '%s', '%s', '%s', '%s', '%s', %s, %s)", data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7], cTax, cYtd);
            String queryNextOId = String.format("INSERT INTO district_next_o_id (W_ID, D_ID, D_NEXT_O_ID) VALUES (%s, %s, %s)", data[0], data[1], data[10]);
            session.execute(query);
            session.execute(queryNextOId);

//            logger.info("Finish processing row: " + dataLine);
            dataLine = br.readLine();
        }

        logger.info("Complete loading district column family!");
    }

    private void loadItemData(Session session) throws IOException {
        String dataLine = br.readLine();
        while (dataLine != null) {
            String[] data = dataLine.split(",");
            String iPrice = new DecimalFormat("0.00").format(new BigDecimal(data[2]));
            String query = String.format("INSERT INTO item (I_ID, I_NAME, I_PRICE, I_IM_ID) VALUES (%s, '%s', %s, %s)", data[0], data[1], iPrice, data[3]);
            String queryUnused = String.format("INSERT INTO item_unused_data (I_ID, I_DATA) VALUES (%s, '%s')", data[0], data[4]);
            session.execute(query);
            session.execute(queryUnused);

//            logger.info("Finish processing row: " + dataLine);
            dataLine = br.readLine();
        }

        logger.info("Complete loading item column family!");
    }

    private void loadOrderData(Session session) throws IOException {
        String dataLine = br.readLine();
        while (dataLine != null) {
            String[] data = dataLine.split(",");
            String oOlCnt = new DecimalFormat("0").format(new BigDecimal(data[5]));
            String oAllLocal = new DecimalFormat("0").format(new BigDecimal(data[6]));
            String query = String.format("INSERT INTO orders (W_ID, D_ID, O_ID, C_ID, O_CARRIER_ID, O_OL_CNT, O_ALL_LOCAL, O_ENTRY_D) VALUES (%s, %s, %s, %s, %s, %s, %s, '%s')", data[0], data[1], data[2], data[3], data[4], oOlCnt, oAllLocal, data[7]);
            session.execute(query);

//            logger.info("Finish processing row: " + dataLine);
            dataLine = br.readLine();
        }

        logger.info("Complete loading order column family!");
    }

    private void loadOrderLineData(Session session) throws IOException {
        String dataLine = br.readLine();
        while (dataLine != null) {
            String[] data = dataLine.split(",");
            String olAmount = new DecimalFormat("0.00").format(new BigDecimal(data[6]));
            String olQuantity = new DecimalFormat("0").format(new BigDecimal(data[8]));
            String query;
            if (data[5].equals("null")) {
                query= String.format("INSERT INTO order_line (W_ID, D_ID, O_ID, OL_NUMBER, I_ID, OL_DELIVERY_D, OL_AMOUNT, OL_SUPPLY_W_ID, OL_QUANTITY) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)", data[0], data[1], data[2], data[3], data[4], "NULL", olAmount, data[7], olQuantity);
            } else {
                query= String.format("INSERT INTO order_line (W_ID, D_ID, O_ID, OL_NUMBER, I_ID, OL_DELIVERY_D, OL_AMOUNT, OL_SUPPLY_W_ID, OL_QUANTITY) VALUES (%s, %s, %s, %s, %s, '%s', %s, %s, %s)", data[0], data[1], data[2], data[3], data[4], data[5], olAmount, data[7], olQuantity);
            }
            String queryUnused = String.format("INSERT INTO order_line_unused_data (W_ID, D_ID, O_ID, OL_NUMBER, OL_DIST_INFO) VALUES (%s, %s, %s, %s, '%s')", data[0], data[1], data[2], data[3], data[9]);
            session.execute(query);
            session.execute(queryUnused);

//            logger.info("Finish processing row: " + dataLine);
            dataLine = br.readLine();
        }

        logger.info("Complete loading order_line column family!");
    }

    private void loadStockData(Session session) throws IOException {
        String dataLine = br.readLine();
        while (dataLine != null) {
            String[] data = dataLine.split(",");
            String sQuantity = new DecimalFormat("0").format(new BigDecimal(data[2]));
            String sYtd = new DecimalFormat("0.00").format(new BigDecimal(data[3]));
            String query = String.format("INSERT INTO stock (W_ID, I_ID, S_QUANTITY, S_YTD, S_ORDER_CNT, S_REMOTE_CNT) VALUES (%s, %s, %s, %s, %s, %s)", data[0], data[1], sQuantity, sYtd, data[4], data[5]);
            String queryConstant = String.format("INSERT INTO stock_constant_data (W_ID, I_ID, S_DIST_01, S_DIST_02, S_DIST_03, S_DIST_04, S_DIST_05, S_DIST_06, S_DIST_07, S_DIST_08, S_DIST_09, S_DIST_10) VALUES (%s, %s, '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s')", data[0], data[1], data[6], data[7], data[8], data[9], data[10], data[11], data[12], data[13], data[14], data[15]);
            String queryUnused = String.format("INSERT INTO stock_unused_data (W_ID, I_ID, S_DATA) VALUES (%s, %s, '%s')", data[0], data[1], data[16]);
            session.execute(query);
            session.execute(queryConstant);
            session.execute(queryUnused);

//            logger.info("Finish processing row: " + dataLine);
            dataLine = br.readLine();
        }

        logger.info("Complete loading stock column family!");
    }

    private void loadWarehouseData(Session session) throws IOException {
        String dataLine = br.readLine();
        while (dataLine != null) {
            String[] data = dataLine.split(",");
            String w_tax = new DecimalFormat("0.0000").format(new BigDecimal(data[7]));
            String w_ytd = new DecimalFormat("0.00").format(new BigDecimal(data[8]));
            String query = String.format("INSERT INTO warehouse (W_ID, W_NAME, W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP, W_TAX, W_YTD) VALUES (%s, '%s', '%s', '%s', '%s', '%s', '%s', %s, %s)", data[0], data[1], data[2], data[3], data[4], data[5], data[6], w_tax, w_ytd);
            session.execute(query);

//            logger.info("Finish processing row: " + dataLine);
            dataLine = br.readLine();
        }

        logger.info("Complete loading warehouse column family!");
    }
}
