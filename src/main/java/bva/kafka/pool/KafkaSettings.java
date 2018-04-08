package bva.kafka.pool;

import java.util.ArrayList;

public class KafkaSettings {
    private Properties driverProps;
    private String topic;
    private Set<Integer> supportedTypes = new HashSet<Integer> ();
    private int sleepLimit=0;
    private int sleepTimeout=1000;
    private ArrayList<Integer> partitions;
    private KafkaOffsetStorageType offsetStorageType = KafkaOffsetStorageType.DEFAULT;
    private String zookeeperServers;
    private String zookeeperOffsetsPath;
    public KafkaSettings(){
        this.partitions = new ArrayList<Integer>();
    }

    public static KafkaSettings create(Properties props) throws IOException {
        // Читаем настройки кафки
        Properties kafkaProps = new Properties ();
        kafkaProps.put ("key.deserializer", StringDeserializer.class.getName ());
        kafkaProps.put ("value.deserializer", StringDeserializer.class.getName ());
        String kafkaConfig = props.getProperty ("kafka_props");
        if (kafkaConfig != null) {
            InputStream kaStream = new FileInputStream (kafkaConfig);
            kafkaProps.load (kaStream);
        }
        KafkaSettings kaSettings = new KafkaSettings ();
        kaSettings.topic = props.getProperty ("topic");
        kaSettings.driverProps = kafkaProps;
        if (props.containsKey ( "sleep_limit" ) && props.containsKey ( "sleep_timeout" )
                ) {
            kaSettings.sleepLimit = Integer.parseInt ( props.getProperty ( "sleep_limit" ) );
            kaSettings.sleepTimeout = Integer.parseInt ( props.getProperty ( "sleep_timeout" ) );
        }
        //Поддерживаемые типы данных
        String spStr = "kafka.supported";
        if (props.containsKey ( spStr )){
            for (String s:props.getProperty(spStr).split ( ",| ")){
                if (!s.isEmpty ()){
                    kaSettings.addSupportedType ( Integer.parseInt ( s ));
                }
            }
        }
        // Хранение оффсетов вне кафки
        if (props.containsKey ( "kafka.offsets.storage" )){
            String storageName = props.getProperty ( "kafka.offsets.storage" );
            if (storageName.equals ("zookeeper")){
                kaSettings.setOffsetStorageType ( KafkaOffsetStorageType.EXTERNAL_ZOOKEEPER );
            }else{
                if (storageName.equals ( "kafka" ))
                    kaSettings.setOffsetStorageType ( KafkaOffsetStorageType.DEFAULT );
                else
                    throw  new IllegalArgumentException ( "Неизвестный параметр в kafka.offsets.storage: " + storageName );
            }
        }
        if (kaSettings.getOffsetStorageType () == KafkaOffsetStorageType.EXTERNAL_ZOOKEEPER){
            String zooServers = props.getProperty ( "kafka.offsets.zookeeper.servers" );
            String zooPath = props.getProperty ( "kafka.offsets.zookeeper.path" );
            if (zooServers==null)
                throw new IllegalArgumentException ( "Не указаны серверы для хранилища оффсетов Zookeeper!" );
            if (zooPath==null)
                throw new IllegalArgumentException ( "Не указан путь для хранения оффсетов в Zookeeper!" );
            kaSettings.zookeeperServers = zooServers;
            kaSettings.zookeeperOffsetsPath = zooPath;
        }
        return kaSettings;
    }

    public Properties getDriverProps () {
        return driverProps;
    }

    public void setDriverProps (Properties driverProps) {
        this.driverProps = driverProps;
    }

    public String getTopic () {
        return topic;
    }

    public void setTopic (String topic) {
        this.topic = topic;
    }

    public final Set <Integer> getSupportedTypes () {
        return supportedTypes;
    }

    private void addSupportedType (Integer t) {
        this.supportedTypes.add ( t );
    }

    public int getSleepLimit () {
        return sleepLimit;
    }

    public void setSleepLimit (int sleepLimit) {
        this.sleepLimit = sleepLimit;
    }

    public int getSleepTimeout () {
        return sleepTimeout;
    }

    public void setSleepTimeout (int sleepTimeout) {
        this.sleepTimeout = sleepTimeout;
    }

    public ArrayList<Integer> getPartitions () {
        return partitions;
    }

    private void setPartitions (ArrayList <Integer> partitions) {
        this.partitions = partitions;
    }

    public KafkaOffsetStorageType getOffsetStorageType () {
        return offsetStorageType;
    }

    public void setOffsetStorageType (KafkaOffsetStorageType offsetStorageType) {
        this.offsetStorageType = offsetStorageType;
    }

    public String getZookeeperServers () {
        return zookeeperServers;
    }

    public String getZookeeperOffsetsPath () {
        return zookeeperOffsetsPath;
    }
}
