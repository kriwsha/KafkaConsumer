package bva.kafka.lib;

import java.util.Map;
import java.util.Properties;

public class Props {
    public static Properties of(Map<String, String> props) {
        Properties p = new Properties();
        props.entrySet().forEach(
                e -> p.setProperty(e.getKey(), e.getValue())
        );
        return p;
    }
}
