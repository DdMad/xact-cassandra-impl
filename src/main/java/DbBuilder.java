import com.datastax.driver.core.Session;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import java.io.*;
import java.util.Properties;

public class DbBuilder {
    private String schemaFilePath;
    private BufferedReader br;

    private Logger logger;

    public DbBuilder(String path) {
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

        schemaFilePath = path;
        try {
            br = new BufferedReader(new FileReader(schemaFilePath));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    public void buildDatabase(Session session) throws IOException {
        if (schemaFilePath == null || schemaFilePath == "" || br == null) {
            System.out.println("Path error!");
            return;
        }

        String query = br.readLine();
        while (query != null) {
            logger.info("Execute cql statement: " + query);
            session.execute(query);
            query = br.readLine();
        }
    }
}
