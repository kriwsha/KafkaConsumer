package bva.kafka.ext;

import java.util.Map;
import java.util.Properties;

public class Props {
    public static Properties of(Map<String, String> props) {
        Properties p = new Properties();
        props.forEach(p::setProperty);
        return p;
    }
}
