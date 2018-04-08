package bva.kafka.pool;

public class KafkaConsumerPool {
    private KafkaSettings settings;
    private int numberOfConsumers;
    private CountDownLatch _latch =null;
    private CancellationToken _cancelToken = new CancellationToken (  );
    private DestinationFactory _factory;
    private static final Logger logger = LogManager.getLogger(KafkaConsumerPool.class);
    private Map<Integer,KafkaWorker> _workers = new  HashMap<Integer,KafkaWorker>();

    public KafkaConsumerPool (int numberOfConsumers, KafkaSettings settings, DestinationFactory factory, String tlvLibraryPath) {
        this.settings = settings;
        if (settings.getTopic ()==null){
            throw new IllegalArgumentException("В конфигурационном файле для Kafka не указана тема для подписки!");
        }
        this.numberOfConsumers = numberOfConsumers;
        this._factory = factory;
        // Инициализируем библиотеку конфигурации
        TlvLibrary.init(tlvLibraryPath);
    }

    public void execute() throws KktImporterException, DestinationCreateException {
        try {
            _latch = new CountDownLatch ( numberOfConsumers ); // Синхронизация потоков
            for (int i = 0; i < numberOfConsumers; i++) {
                KafkaWorker w = createConsumerWorker ( );
                logger.info ( "Запущен поток " + w.getName ( ) + " " + w.getThreadGroup ( ) + " " + w.getId ( ) );
                _workers.put ( i, w );
            }
            for (KafkaWorker w : _workers.values ( )) {
                w.start ( );
            }
        }
        catch (Exception e){
            _cancelToken.closeAll ();
            logger.error ( e );
            throw e;
        }
        finally{
            // Ожидаем остановки всех потомков
            try {
                _latch.await ();
                // Если есть ошибка, прокидываем ее выше
                if (_cancelToken.hasException ())
                    throw new KktImporterException ( _cancelToken.getLastException () );
            } catch (InterruptedException e) {
                e.printStackTrace ( );
            }
        }
    }

    @Override
    public void stop() {
        _workers.values ().forEach ( w->w.shutdown () );
        try {
            _latch.await ();
        } catch (Exception e) {
            e.printStackTrace ( );
            logger.fatal ( "Прерывание в главном потоке" );
        }
    }

    private KafkaWorker createConsumerWorker() throws DestinationCreateException {
        try{
            return new KafkaWorker(this.settings, _factory, _latch,_cancelToken);
        } catch(Exception e){
            _latch.countDown (); // Минусуем счетчик, т.к. создать поток не получилось
            throw  e;
        }
    }
}
