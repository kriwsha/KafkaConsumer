package bva.kafka.pool;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

public class KafkaConsumerPool {
    private KafkaSettings settings;
    private int numberOfConsumers;
    private CountDownLatch latch;
    private CancellationToken cancelToken = new CancellationToken();
    private DestinationFactory factory;
    private Map<Integer,KafkaWorker> workers = new HashMap<>();

    public KafkaConsumerPool (int numberOfConsumers, KafkaSettings settings, DestinationFactory factory, String tlvLibraryPath) {
        this.settings = settings;
        if (settings.getTopic ()==null){
            throw new IllegalArgumentException("В конфигурационном файле для Kafka не указана тема для подписки!");
        }
        this.numberOfConsumers = numberOfConsumers;
        this.factory = factory;
        // Инициализируем библиотеку конфигурации
        TlvLibrary.init(tlvLibraryPath);
    }

    public void execute() throws Exception{
        try {
            latch = new CountDownLatch (numberOfConsumers); // Синхронизация потоков
            for (int i = 0; i < numberOfConsumers; i++)
                workers.put (i, createConsumerWorker());
            for (KafkaWorker w : workers.values())
                w.start();
        }
        catch (Exception e){
            cancelToken.closeAll();
            throw e;
        }
        finally{
            // Ожидаем остановки всех потомков
            try {
                latch.await ();
                // Если есть ошибка, прокидываем ее выше
                if (cancelToken.hasException ())
                    throw new Exception (cancelToken.getLastException());
            } catch (InterruptedException e) {
                e.printStackTrace ( );
            }
        }
    }

    public void stop() {
        workers.values().forEach(KafkaWorker::shutdown);
        try {
            latch.await ();
        } catch (Exception ex) {
            ex.printStackTrace ( );
        }
    }

    private KafkaWorker createConsumerWorker() throws Exception{
        try{
            return new KafkaWorker(this.settings, factory, latch, cancelToken);
        } catch(Exception e){
            latch.countDown (); // Минусуем счетчик, т.к. создать поток не получилось
            throw  e;
        }
    }
}
