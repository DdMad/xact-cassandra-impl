import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import java.io.*;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Properties;

/**
 * Created by ddmad on 13/10/16.
 */
public class XactProcessor {
    private String xactFileDir;
    private BufferedReader br;
    private BufferedWriter bw;

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

        File output = new File(xactFileDir + "-out.txt");
        if (!output.exists()) {
            output.createNewFile();
        }
        bw = new BufferedWriter(new FileWriter(output));

        String dataLine = br.readLine();
        while (dataLine != null) {
            String[] data = dataLine.split(",");
            String type = data[0];
            if (type.equals("N")) {
                processNewOrderXact(session, data);
            } else if (type.equals("P")) {
                processPaymentXact(session, data);
            } else if (type.equals("D")) {
                processDeliveryXact(session, data);
            } else if (type.equals("O")) {
                processOrderStatusXact(session, data);
            } else if (type.equals("S")) {
                processStockLevelXact(session, data);
            } else if (type.equals("I")) {
                processPopularItemXact(session, data);
            } else if (type.equals("T")) {
                processTopBalanceXact(session, data);
            } else {
                logger.warn("Wrong transaction type!");
            }

            logger.info("Finish processing transaction: " + dataLine);
            dataLine = br.readLine();
        }
    }

    private void processNewOrderXact(Session session, String[] data) throws IOException {
        String cId = data[1];
        String wId = data[2];
        String dId = data[3];
        int m = Integer.parseInt(data[4]);

        BigDecimal totalAmount = new BigDecimal(0);
        int oAllLocal = 1;
        ArrayList<String> itemNumberList = new ArrayList<>();
        ArrayList<String> iNameList = new ArrayList<>();
        ArrayList<String> supplyWarehouseList = new ArrayList<>();
        ArrayList<String> quantityList = new ArrayList<>();
        ArrayList<String> olAmountList = new ArrayList<>();
        ArrayList<String> sQuantityList = new ArrayList<>();

        // Get D_NEXT_O_ID
        int oId = session.execute(String.format("SELECT D_NEXT_O_ID FROM district_next_o_id WHERE W_ID = %s AND D_ID = %s", wId, dId)).one().getInt("D_NEXT_O_ID");

        // Update D_NEXT_O_ID by adding one
        session.execute(String.format("UPDATE district_next_o_id SET D_NEXT_O_ID = %d WHERE W_ID = %s AND D_ID = %s", oId + 1, wId, dId));

        for (int i = 0; i < m; i++) {
            String[] itemData = br.readLine().split(",");
            String olIId = itemData[0];
            String olSupplyWId = itemData[1];
            String olQuantity = itemData[2];

            // Check if it is not local warehouse
            oAllLocal = olSupplyWId.equals(wId) ? 1 : 0;

            // Get stock quantity
            Row stock = session.execute(String.format("SELECT S_QUANTITY, S_YTD, S_ORDER_CNT, S_REMOTE_CNT FROM stock WHERE W_ID = %s AND I_ID = %s", olSupplyWId, olIId)).one();
            BigDecimal sQuantity = stock.getDecimal("S_QUANTITY");

            // Calculate adjusted quantity
            BigDecimal adjustedQty = sQuantity.subtract(new BigDecimal(Integer.parseInt(olQuantity)));
            adjustedQty = adjustedQty.compareTo(new BigDecimal(10)) == -1 ? adjustedQty.add(new BigDecimal(100)) : adjustedQty;

            // Update stock
            BigDecimal sYtd = stock.getDecimal("S_YTD");
            int sOrderCnt = stock.getInt("S_ORDER_CNT");
            int sRemoteCnt = stock.getInt("S_REMOTE_CNT");
            sRemoteCnt = olSupplyWId.equals(wId) ? sRemoteCnt : sRemoteCnt + 1;
            session.execute(String.format("UPDATE stock SET S_QUANTITY = %s, S_YTD = %s, S_ORDER_CNT = %d, S_REMOTE_CNT = %d WHERE W_ID = %s AND I_ID = %s", adjustedQty.toPlainString(), sYtd.add(sQuantity).toPlainString(), sOrderCnt + 1, sRemoteCnt, olSupplyWId, olIId));

            // Calculate total amount
            Row item = session.execute(String.format("SELECT I_PRICE, I_NAME FROM item WHERE I_ID = %s", olIId)).one();
            BigDecimal iPrice = item.getDecimal("I_PRICE");
            BigDecimal itemAmount = new BigDecimal(olQuantity).multiply(iPrice);
            totalAmount = totalAmount.add(itemAmount);

            // Create a new order-line (May improve the stock)
            String columnName = "S_DIST_" + (Integer.parseInt(dId) == 10 ? "10" : ("0" + dId));
            String sDist = session.execute(String.format("SELECT %s FROM stock_constant_data WHERE W_ID = %s AND I_ID = %s", columnName, olSupplyWId, olIId)).one().getString(columnName);
            session.execute(String.format("INSERT INTO order_line (W_ID, D_ID, O_ID, OL_NUMBER, I_ID, OL_DELIVERY_D, OL_AMOUNT, OL_SUPPLY_W_ID, OL_QUANTITY) VALUES (%s, %s, %s, %d, %s, %s, %s, %s, %s)", wId, dId, oId, i, olIId, "NULL", itemAmount.toPlainString(), olSupplyWId, olQuantity));
            session.execute(String.format("INSERT INTO order_line_unused_data (W_ID, D_ID, O_ID, OL_NUMBER, OL_DIST_INFO) VALUES (%s, %s, %s, %s, '%s')", wId, dId, oId, i, sDist));

            // Add output information to list
            itemNumberList.add(olIId);
            iNameList.add(item.getString("I_NAME"));
            supplyWarehouseList.add(olSupplyWId);
            quantityList.add(olQuantity);
            olAmountList.add(itemAmount.toPlainString());
            sQuantityList.add(adjustedQty.toPlainString());
        }

        // Create new order
        String currentTime = new Timestamp(System.currentTimeMillis()).toString();
        session.execute(String.format("INSERT INTO orders (W_ID, D_ID, O_ID, C_ID, O_CARRIER_ID, O_OL_CNT, O_ALL_LOCAL, O_ENTRY_D) VALUES (%s, %s, %d, %s, %s, %s, %s, '%s')", wId, dId, oId, cId, "NULL", m, oAllLocal, currentTime));

        // Get customer information
        Row customer = session.execute(String.format("SELECT C_LAST, C_CREDIT, C_DISCOUNT FROM customer_constant_data WHERE W_ID = %s AND D_ID = %s AND C_ID = %s", wId, dId, cId)).one();
        // Get w_tax and d_tax
        BigDecimal wTax = session.execute(String.format("SELECT W_TAX FROM warehouse WHERE W_ID = %s", wId)).one().getDecimal("W_TAX");
        BigDecimal dTax = session.execute(String.format("SELECT D_TAX FROM district WHERE W_ID = %s AND D_ID = %s", wId, dId)).one().getDecimal("D_TAX");
        // Compute final total amount
        totalAmount = totalAmount.multiply(new BigDecimal(1).add(dTax).add(wTax)).multiply(new BigDecimal(1).subtract(customer.getDecimal("C_DISCOUNT")));

        // Write output
        bw.write(String.format("%s,%s,%s,%s,%s,%s", wId, dId, cId, customer.getString("C_LAST"), customer.getString("C_CREDIT"), customer.getDecimal("C_DISCOUNT").toPlainString()));
        bw.newLine();
        bw.write(String.format("%s,%s", wTax.toPlainString(), dTax.toPlainString()));
        bw.newLine();
        bw.write(String.format("%s,%s", oId, currentTime));
        bw.newLine();
        bw.write(String.format("%d,%s", m, totalAmount.toPlainString()));
        bw.newLine();
        for (int i = 0; i < m; i++) {
            bw.write(String.format("%s,%s,%s,%s,%s,%s", itemNumberList.get(i), iNameList.get(i), supplyWarehouseList.get(i), quantityList.get(i), olAmountList.get(i), sQuantityList.get(i)));
            bw.newLine();
        }
    }

    private void processPaymentXact(Session session, String[] data) {

    }

    private void processDeliveryXact(Session session, String[] data) {

    }

    private void processOrderStatusXact(Session session, String[] data) {

    }

    private void processStockLevelXact(Session session, String[] data) {

    }

    private void processPopularItemXact(Session session, String[] data) {

    }

    private void processTopBalanceXact(Session session, String[] data) {

    }
}
