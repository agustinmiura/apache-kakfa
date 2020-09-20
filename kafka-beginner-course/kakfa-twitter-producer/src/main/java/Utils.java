package ar.com.miura;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class Utils {

    private static final Logger LOGGER = LoggerFactory.getLogger(Utils.class.getName());

    public static Properties readPropertiesFile(String fileName, Class clazz) throws IOException {
        Properties prop = null;
        try(InputStream is = clazz.getClassLoader().getResourceAsStream(fileName)) {
            prop = new Properties();
            prop.load(is);
        } catch(FileNotFoundException fnfe) {
            LOGGER.error(" Error ", fnfe);
        } catch(IOException ioe) {
            LOGGER.error(" IOException ", ioe);
        }
        return prop;
    }
}
