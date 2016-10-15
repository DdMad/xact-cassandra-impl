import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import java.io.*;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

/**
 * Created by ddmad on 13/10/16.
 */
public class XactProcessor {
    private String xactFileDir;
    private BufferedReader br;
    private BufferedWriter bw;

    private Logger logger;

    public XactProcessor(String dir) {
        logger = Logger.getLogger(XactProcessor.class.getName());
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
                processTopBalanceXact(session);
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

        // Calculate for popular
        String popularIName = "";
        BigDecimal popularQuantity = new BigDecimal("0");
        Set<String> itemSet = new HashSet<>();

        // Get D_NEXT_O_ID
        int oId = session.execute(String.format("SELECT D_NEXT_O_ID FROM district_next_o_id WHERE W_ID = %s AND D_ID = %s", wId, dId)).one().getInt("D_NEXT_O_ID");

        // Update D_NEXT_O_ID by adding one
        session.execute(String.format("UPDATE district_next_o_id SET D_NEXT_O_ID = %d WHERE W_ID = %s AND D_ID = %s", oId + 1, wId, dId));

        for (int i = 1; i <= m; i++) {
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
            session.execute(String.format("INSERT INTO order_line (W_ID, D_ID, O_ID, OL_NUMBER, I_ID, OL_DELIVERY_D, OL_AMOUNT, OL_SUPPLY_W_ID, OL_QUANTITY) VALUES (%s, %s, %d, %d, %s, %s, %s, %s, %s)", wId, dId, oId, i, olIId, "NULL", itemAmount.toPlainString(), olSupplyWId, olQuantity));
            session.execute(String.format("INSERT INTO order_line_unused_data (W_ID, D_ID, O_ID, OL_NUMBER, OL_DIST_INFO) VALUES (%s, %s, %d, %d, '%s')", wId, dId, oId, i, sDist));

            // Add output information to list
            itemNumberList.add(olIId);
            iNameList.add(item.getString("I_NAME"));
            supplyWarehouseList.add(olSupplyWId);
            quantityList.add(olQuantity);
            olAmountList.add(itemAmount.toPlainString());
            sQuantityList.add(adjustedQty.toPlainString());

            // Calculate for popular
            String iName = item.getString("I_NAME");
            itemSet.add(iName);

            if (popularIName.equals("") || popularQuantity.compareTo(new BigDecimal(olQuantity)) < 0) {
                popularQuantity = new BigDecimal(olQuantity);
                popularIName = iName;
            }
        }

        // Convert Set to String
        StringBuilder sb = new StringBuilder();
        sb.append('{');
        for (String s : itemSet) {
            sb.append('\'');
            sb.append(s);
            sb.append('\'');
            sb.append(',');
        }
        sb.replace(sb.length() - 1, sb.length(), "}");

        // Create new order
        String currentTime = new Timestamp(System.currentTimeMillis()).toString();
        session.execute(String.format("INSERT INTO orders (W_ID, D_ID, O_ID, C_ID, O_CARRIER_ID, O_OL_CNT, O_ALL_LOCAL, O_ENTRY_D, O_POPULAR_I_NAME, O_POPULAR_OL_QUANTITY, O_ITEM_SET) VALUES (%s, %s, %d, %s, %d, %s, %s, '%s', '%s', %s, %s)", wId, dId, oId, cId, -1, m, oAllLocal, currentTime, popularIName, popularQuantity.toPlainString(), sb.toString()));

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
        bw.write(String.format("%d,%s", oId, currentTime));
        bw.newLine();
        bw.write(String.format("%d,%s", m, totalAmount.toPlainString()));
        bw.newLine();
        for (int i = 0; i < m; i++) {
            bw.write(String.format("%s,%s,%s,%s,%s,%s", itemNumberList.get(i), iNameList.get(i), supplyWarehouseList.get(i), quantityList.get(i), olAmountList.get(i), sQuantityList.get(i)));
            bw.newLine();
        }
        bw.flush();
    }

    private void processPaymentXact(Session session, String[] data) throws IOException {
        String wId = data[1];
        String dId = data[2];
        String cId = data[3];
        String payment = data[4];

        // Get all data
        Row warehouse = session.execute(String.format("SELECT W_YTD, W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP FROM warehouse WHERE W_ID = %s", wId)).one();
        Row district = session.execute(String.format("SELECT D_YTD, D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP FROM district WHERE W_ID = %s AND D_ID = %s", wId, dId)).one();
        Row customer = session.execute(String.format("SELECT C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_DELIVERY_CNT FROM customer WHERE W_ID = %s AND D_ID = %s AND C_ID = %s", wId, dId, cId)).one();
        Row customerConstant = session.execute(String.format("SELECT * FROM customer_constant_data WHERE W_ID = %s AND D_ID = %s AND C_ID = %s", wId, dId, cId)).one();

        // Compute new balance
        BigDecimal newBalance = customer.getDecimal("C_BALANCE").subtract(new BigDecimal(payment));

        // Update W_YTD and D_YTD
        session.execute(String.format("UPDATE warehouse SET W_YTD = %s WHERE W_ID = %s", warehouse.getDecimal("W_YTD").add(new BigDecimal(payment)).toPlainString(), wId));
        session.execute(String.format("UPDATE district SET D_YTD = %s WHERE W_ID = %s AND D_ID = %s", district.getDecimal("D_YTD").add(new BigDecimal(payment)).toPlainString(), wId, dId));

        // Update C_BALANCE C_YTD_PAYMENT C_PAYMENT_CNT (delete old one and insert new one)
        session.execute(String.format("DELETE FROM customer WHERE W_ID = %s AND D_ID = %s AND C_ID = %s", wId, dId, cId));
        session.execute(String.format("INSERT INTO customer (W_ID, D_ID, C_ID, C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_DELIVERY_CNT) VALUES (%s, %s, %s, %s, %f, %d, %d)", wId, dId, cId, newBalance.toPlainString(), customer.getFloat("C_YTD_PAYMENT") + Float.parseFloat(payment), customer.getInt("C_PAYMENT_CNT") + 1, customer.getInt("C_DELIVERY_CNT")));

        // Write output
        bw.write(String.format("%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s", wId, dId, cId, customerConstant.getString("C_FIRST"), customerConstant.getString("C_MIDDLE"), customerConstant.getString("C_LAST"), customerConstant.getString("C_STREET_1"), customerConstant.getString("C_STREET_2"), customerConstant.getString("C_CITY"), customerConstant.getString("C_STATE"), customerConstant.getString("C_ZIP"), customerConstant.getString("C_PHONE"), customerConstant.getTimestamp("C_SINCE").toString(), customerConstant.getString("C_CREDIT"), customerConstant.getDecimal("C_CREDIT_LIM").toPlainString(), customerConstant.getDecimal("C_DISCOUNT").toPlainString(), newBalance));
        bw.newLine();
        bw.write(String.format("%s,%s,%s,%s,%s", warehouse.getString("W_STREET_1"), warehouse.getString("W_STREET_2"), warehouse.getString("W_CITY"), warehouse.getString("W_STATE"), warehouse.getString("W_ZIP")));
        bw.newLine();
        bw.write(String.format("%s,%s,%s,%s,%s", district.getString("D_STREET_1"), district.getString("D_STREET_2"), district.getString("D_CITY"), district.getString("D_STATE"), district.getString("D_ZIP")));
        bw.newLine();
        bw.write(String.format("%s", payment));
        bw.newLine();
        bw.flush();
    }

    private void processDeliveryXact(Session session, String[] data) {
        String wId = data[1];
        String carrierId = data[2];

        // For each district
        for (int i = 1; i <= 10; i++) {
            // Get smallest O_ID
            Row order = session.execute(String.format("SELECT MIN(O_ID), C_ID, O_OL_CNT FROM orders WHERE W_ID = %s AND D_ID = %d AND O_CARRIER_ID = -1 ALLOW FILTERING", wId, i)).one();
            int oId = order.getInt("system.min(O_ID)");

            // Update O_CARRIER_ID
            session.execute(String.format("UPDATE orders SET O_CARRIER_ID = %s WHERE W_ID = %s AND D_ID = %d AND O_ID = %d", carrierId, wId, i, oId));

            // Update all order-lines
            String currentTime = new Timestamp(System.currentTimeMillis()).toString();
            int m = order.getDecimal("O_OL_CNT").intValue();
            for (int j = 1; j <= m; j++) {
                session.execute(String.format("UPDATE order_line SET OL_DELIVERY_D = '%s' WHERE W_ID = %s AND D_ID = %d AND O_ID = %d AND OL_NUMBER = %d", currentTime, wId, i, oId, j));
            }

            // Update customer
            BigDecimal b = session.execute(String.format("SELECT SUM(OL_AMOUNT) FROM order_line WHERE W_ID = %s AND D_ID = %d AND O_ID = %d", wId, i, oId)).one().getDecimal(0);
            int cId = order.getInt("C_ID");
            Row customer = session.execute(String.format("SELECT * FROM customer WHERE W_ID = %s AND D_ID = %d AND C_ID = %d", wId, i, cId)).one();
            session.execute(String.format("DELETE FROM customer WHERE W_ID = %s AND D_ID = %d AND C_ID = %d", wId, i, cId));
            session.execute(String.format("INSERT INTO customer (W_ID, D_ID, C_ID, C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_DELIVERY_CNT) VALUES (%s, %d, %d, %s, %f, %d, %d)", wId, i, cId, customer.getDecimal("C_BALANCE").add(b).toPlainString(), customer.getFloat("C_YTD_PAYMENT"), customer.getInt("C_PAYMENT_CNT"), customer.getInt("C_DELIVERY_CNT") + 1));
        }
    }

    private void processOrderStatusXact(Session session, String[] data) throws IOException {
        String wId = data[1];
        String dId = data[2];
        String cId = data[3];

        // Get customer name and balance
        BigDecimal cBalance = session.execute(String.format("SELECT C_BALANCE FROM customer WHERE W_ID = %s AND D_ID = %s AND C_ID = %s", wId, dId, cId)).one().getDecimal("C_BALANCE");
        Row customerConstant = session.execute(String.format("SELECT C_FIRST, C_MIDDLE, C_LAST FROM customer_constant_data WHERE W_ID = %s AND D_ID = %s AND C_ID = %s", wId, dId, cId)).one();

        // Get last order of customer
        Row lastOrder = session.execute(String.format("SELECT MAX(O_ID), O_ENTRY_D, O_CARRIER_ID, O_OL_CNT FROM orders WHERE W_ID = %s AND D_ID = %s AND C_ID = %s ALLOW FILTERING", wId, dId, cId)).one();
        int oId = lastOrder.getInt("system.max(O_ID)");

        // Write output
        bw.write(String.format("%s,%s,%s,%s", customerConstant.getString("C_FIRST"), customerConstant.getString("C_MIDDLE"), customerConstant.getString("C_LAST"), cBalance.toPlainString()));
        bw.newLine();
        bw.write(String.format("%d,%s,%d", oId, lastOrder.getTimestamp("O_ENTRY_D").toString(), lastOrder.getInt("O_CARRIER_ID")));
        bw.newLine();

        // For each item in last order of customer
        int m = lastOrder.getDecimal("O_OL_CNT").intValue();
        System.out.println(m);
        for (int i = 1; i <= m; i++) {
            // Get order-line
            Row orderLine = session.execute(String.format("SELECT I_ID, OL_SUPPLY_W_ID, OL_QUANTITY, OL_AMOUNT, OL_DELIVERY_D FROM order_line WHERE W_ID = %s AND D_ID = %s AND O_ID = %d AND OL_NUMBER = %d", wId, dId, oId, i)).one();

            System.out.println(String.format("SELECT I_ID, OL_SUPPLY_W_ID, OL_QUANTITY, OL_AMOUNT, OL_DELIVERY_D FROM order_line WHERE W_ID = %s AND D_ID = %s AND O_ID = %d AND OL_NUMBER = %d", wId, dId, oId, i));
            // Write output
            if (orderLine == null) {
                break;
            } else if (orderLine.getTimestamp("OL_DELIVERY_D") == null) {
                bw.write(String.format("%d,%d,%s,%s,%s", orderLine.getInt("I_ID"), orderLine.getInt("OL_SUPPLY_W_ID"), orderLine.getDecimal("OL_QUANTITY").toPlainString(), orderLine.getDecimal("OL_AMOUNT").toPlainString(), "NULL"));
            } else {
                bw.write(String.format("%d,%d,%s,%s,%s", orderLine.getInt("I_ID"), orderLine.getInt("OL_SUPPLY_W_ID"), orderLine.getDecimal("OL_QUANTITY").toPlainString(), orderLine.getDecimal("OL_AMOUNT").toPlainString(), orderLine.getTimestamp("OL_DELIVERY_D").toString()));
            }
            bw.newLine();
        }
        bw.flush();
    }

    private void processStockLevelXact(Session session, String[] data) throws IOException {
        String wId = data[1];
        String dId = data[2];
        BigDecimal stockThreshold = new BigDecimal(data[3]);
        int lastItemAmount = Integer.parseInt(data[4]);

        int count = 0;

        // Get next available O_ID
        int nextOId= session.execute(String.format("SELECT D_NEXT_O_ID FROM district_next_o_id WHERE W_ID = %s AND D_ID = %s", wId, dId)).one().getInt("D_NEXT_O_ID");

        for (int i = nextOId - lastItemAmount; i < nextOId; i++) {
            // Get number of order-line
            int m = session.execute(String.format("SELECT O_OL_CNT FROM orders WHERE W_ID = %s AND D_ID = %s AND O_ID = %d", wId, dId, i)).one().getDecimal("O_OL_CNT").intValue();

            for (int j = 1; j <= m; j++) {
                // Get I_ID
                int iId = session.execute(String.format("SELECT I_ID FROM order_line WHERE W_ID = %s AND D_ID = %s AND O_ID = %d AND OL_NUMBER = %d", wId, dId, i, j)).one().getInt("I_ID");

                // Get S_QUANTITY
                BigDecimal sQuantity = session.execute(String.format("SELECT S_QUANTITY FROM stock WHERE W_ID = %s AND I_ID = %d", wId, iId)).one().getDecimal("S_QUANTITY");

                count = sQuantity.compareTo(stockThreshold) < 0 ? count + 1 : count;
            }
        }

        // Write output
        bw.write(String.format("%d", count));
        bw.newLine();
        bw.flush();
    }

    private void processPopularItemXact(Session session, String[] data) throws IOException {
        String wId = data[1];
        String dId = data[2];
        int lastOrderAmount = Integer.parseInt(data[3]);

        // Get next available O_ID
        int nextOId= session.execute(String.format("SELECT D_NEXT_O_ID FROM district_next_o_id WHERE W_ID = %s AND D_ID = %s", wId, dId)).one().getInt("D_NEXT_O_ID");

        Set<String> itemList = new HashSet<>();
        ArrayList<Set<String>> itemSetList = new ArrayList<>();

        // Write output
        bw.write(String.format("%s,%s", wId, dId));
        bw.newLine();
        bw.write(String.format("%d", lastOrderAmount));
        bw.newLine();

        for (int i = nextOId - lastOrderAmount; i < nextOId; i++) {
            // Get order
            Row order = session.execute(String.format("SELECT C_ID, O_ENTRY_D, O_POPULAR_I_NAME, O_POPULAR_OL_QUANTITY, O_ITEM_SET FROM orders WHERE W_ID = %s AND D_ID = %s AND O_ID = %d", wId, dId, i)).one();
            Row customer = session.execute(String.format("SELECT C_FIRST, C_MIDDLE, C_LAST FROM customer_constant_data WHERE W_ID = %s AND D_ID = %s AND C_ID = %d", wId, dId, order.getInt("C_ID"))).one();

            if (order.getString("O_POPULAR_I_NAME") == null || order.getDecimal("O_POPULAR_OL_QUANTITY") == null || order.getSet("O_ITEM_SET", String.class) == null) {
                continue;
            }

            bw.write(String.format("%d,%s,%s,%s,%s,%s,%s", i, order.getTimestamp("O_ENTRY_D").toString(), customer.getString("C_FIRST"), customer.getString("C_MIDDLE"), customer.getString("C_LAST"), order.getString("O_POPULAR_I_NAME"), order.getDecimal("O_POPULAR_OL_QUANTITY").toPlainString()));
            bw.newLine();

            itemList.add(order.getString("O_POPULAR_I_NAME"));
            itemSetList.add(order.getSet("O_ITEM_SET", String.class));
        }

        // Calculate percentage
        for (String item : itemList) {
            int count = 0;

            for (Set<String> s : itemSetList) {
                count = s.contains(item) ? count + 1 : count;
            }

            float percentage = ((float) count) / ((float) lastOrderAmount);
            bw.write(String.format("%s,%f", item, percentage));
            bw.newLine();
        }
        bw.flush();
    }

    private void processTopBalanceXact(Session session) throws IOException {
        // Get top 10 customers
        ResultSet top10 = session.execute(String.format("SELECT W_ID, D_ID, C_ID, C_BALANCE FROM customer LIMIT 10"));
        for (Row r : top10) {
            // Get name
            Row customerConstant = session.execute(String.format("SELECT C_FIRST, C_MIDDLE, C_LAST FROM customer_constant_data WHERE W_ID = %d AND D_ID = %d AND C_ID = %d", r.getInt("W_ID"), r.getInt("D_ID"), r.getInt("C_ID"))).one();
            String wName = session.execute(String.format("SELECT W_NAME FROM warehouse WHERE W_ID = %d", r.getInt("W_ID"))).one().getString("W_NAME");
            String dName = session.execute(String.format("SELECT D_NAME FROM district WHERE W_ID = %d AND D_ID = %d", r.getInt("W_ID"), r.getInt("D_ID"))).one().getString("D_NAME");

            // Get balance
            BigDecimal cBalance = session.execute(String.format("SELECT C_BALANCE FROM customer WHERE W_ID = %d AND D_ID = %d AND C_ID = %d", r.getInt("W_ID"), r.getInt("D_ID"), r.getInt("C_ID"))).one().getDecimal("C_BALANCE");

            // Write output
            bw.write(String.format("%s,%s,%s,%s,%s,%s", customerConstant.getString("C_FIRST"), customerConstant.getString("C_MIDDLE"), customerConstant.getString("C_LAST"), cBalance.toPlainString(), wName, dName));
            bw.newLine();
        }
        bw.flush();
    }
}
