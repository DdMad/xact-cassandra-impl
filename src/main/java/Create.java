import com.datastax.driver.core.Session;

public class Create {
    static String[] queries = new String[]{
            // Store the W_YTD value for each Warehouse
            "CREATE TABLE w_ytd(W_ID int PRIMARY KEY, W_YID int);",

            // Store the D_YTD value for each Warehouse
            "CREATE TABLE d_ytd(D_ID int PRIMARY KEY, D_YTD int);",

            // Store all the information that only need to be read of Warehouse
            "CREATE TABLE warehouse(W_ID int PRIMARY KEY, W_TAX text, W_NAME text, W_STREET_1 text, W_STREET_2 text, " +
                    "W_CITY text, W_STATE text, W_ZIP text);",

            // Store all the information that only need to be read of District
            "CREATE TABLE district(D_ID int PRIMARY KEY, D_TAX text, D_NAME text, D_ADDRESS text);",

            // Store all the information that only need to be read of Customer
            "CREATE TABLE customer(W_ID int PRIMARY KEY, C_ID int, C_FIRST text, C_LAST text, C_SINCE int," +
                    "C_ADDRESS text, C_CREDIT int, C_CREDIT_LIM int, C_DISCOUNT int, C_DATA text);",

            // Store all the Item information. We will have one copy for all Items in each node
            "CREATE TABLE item(I_ID int PRIMARY KEY, I_NAME text, I_PRICE int," +
                    "I_IM_ID int, I_DATA text);",

            // Store all the information that may be only read (no writing) of Stock
            "CREATE TABLE stock(S_ID int PRIMARY KEY, S_DIST_01 text, S_DIST_02 text," +
                    "S_DIST_03 text, S_DIST_04 text, S_DIST_05 text, S_DIST_06 text," +
                    "S_DIST_07 text, S_DIST_08 text, S_DIST_09 text, S_DATA text);",

            // This table is used to get next order number for the district
            "CREATE TABLE next_order_district(W_ID int PRIMARY KEY, D_NEXT_O_ID int);",

            // This table stores the data that might be updated by some transactions (e.g. Payment Transactions) of Customer
            "CREATE TABLE customer_transactions(WD_ID int PRIMARY KEY, C_ID int, C_BALANCE int, C_YTD int, C_PAYMENT_CNT int, C_DELIVERY_CNT int);",

            // Store all the Order information in this table
            "CREATE TABLE orders(O_ID int PRIMARY KEY, WD_ID int, C_ID int, CARRIER_ID int, OL_CNT text, ALL_LOCAL text, ENTRY_D text);",

            // Store all the OrderLine information in this table
            "CREATE TABLE order_line(WD_ID int PRIMARY KEY, OL_O_ID int, OL_NUMBER int, OL_I_ID int, DELIVERY_D text," +
                    "AMOUNT int, SUPPLY_W_ID int, OL_QUANTITY int, OL_DIST_INFO text);",

            // This table stores the data of Stock that may need to be updated through some transactions.
            "CREATE TABLE stock_transaction(S_ID int PRIMARY KEY, W_ID int, I_ID int, S_QUANTITY int, S_YTD int," +
                    "S_ORDER_CNT text, S_REMOTE_CNT text);"
    };
 
    public static void createTables(Session session) {
        for (String query : Create.queries) {
            session.execute(query);
        }
    }
}
