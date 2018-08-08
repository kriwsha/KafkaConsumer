package bva.kafka.pool;

import bva.kafka.exceptions.KafkaSourceException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.logging.log4j.message.MapMessage;

import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

class KafkaTlvLibraryException extends KafkaSourceException {
    public KafkaTlvLibraryException() {}
    public KafkaTlvLibraryException(String msg) {
        super(msg);
    }
    public KafkaTlvLibraryException(Exception e) {
        super(e);
    }
}

class CancellationToken{
    private AtomicBoolean bCancel = new AtomicBoolean ();
    private Exception exception = null;

    public Exception getLastException(){
        return exception;
    }
    public void  setLastException(Exception e){
        exception = e;
    }
    public Boolean hasException(){
        return exception!=null;
    }
    public Boolean getState(){
        return bCancel.get ();
    }

    public void closeAll (){
        bCancel.set ( true );
    }
}

public class KafkaWorker extends Thread  {
    private static final Logger logger = LogManager.getLogger(KafkaWorker.class);
    private KafkaSettings _settings;
    private CancellationToken cancellationToken = new CancellationToken ();
    ArrayList <KktDocument> _buffer = new ArrayList <KktDocument> ( );
    private final KafkaConsumer<String, String> consumer;
    private KktDestination _destination;
    private IKafkaOffsetStorage _offsetStorage = null;
    private final CountDownLatch _latch;

    public KafkaWorker(KafkaSettings settings, DestinationFactory factory, CountDownLatch latch, CancellationToken cancellationToken) throws DestinationCreateException {
        try {
            consumer = new KafkaConsumer <String, String> ( settings.getDriverProps () );
            _settings = settings;
            _destination = factory.createDestination ();
            // Создаем объект для хранения оффсетов, если необходимо
            _latch = latch;
            this.cancellationToken = cancellationToken;
        } catch (Exception e){
            throw new DestinationCreateException ( e );
        }
    }

    public void run() {
        try {
            _buffer.clear ();
            final Integer poolTimeout =  10000;
            //Открываем приемник
            _destination.open ();
            // Тестирование приемника
            if (!_destination.isReady ()) {
                logger.fatal ( "Приемник сообщений не готов");
                throw new DestinationNotReadyException ("Приемник сообщений не готов");
            }
            if (_settings.getOffsetStorageType () != KafkaOffsetStorageType.DEFAULT ) {
                _offsetStorage = createOffsetStorage ( );
                // Подписка на тему в Кафка вместе с контролером событий
                consumer.subscribe ( Arrays.asList ( _settings.getTopic () ) , new OffsetsController ( consumer, _offsetStorage,cancellationToken ));
            }
            else{
                consumer.subscribe ( Arrays.asList ( _settings.getTopic () ));
            }
            Gson gson = new Gson ( );
            int prevCount = 0;
            // Засыпаем в первый раз на случайное время для равномерного распределения нагрузки между потоками
            /*Random rnd = new Random();
               int sleepTimeout = rnd.nextInt ( _settings.getSleepTimeout ()  );*/
            int sleepTimeout = 0;
            //Основной цикл
            while (!(cancellationToken.getState ())) {
                //Получение записей
                logger.debug ( "Receive new records from Apache Kafka" );
                ConsumerRecords<String, String> records = consumer.poll (poolTimeout);
                prevCount = records.count ( );
                logger.debug ( "Received " + records.count ( ) + " records" );
                // Обрабатываем все полученные сообщения
                for (ConsumerRecord<String, String> record : records) {
                    DecryptedMessage msg = null;
                    try {
                        if (record.value ().isEmpty ())
                            throw new KafkaTlvLibraryException ( "Тело сообщения пустое: партиция +" + record.partition ()+" смещение " + record.offset () );
                        msg = gson.fromJson ( record.value ( ), DecryptedMessage.class );
                        // Если указаны поддерживаемые типы документов, обрабатываем только сообщения с соответствующим tid
                        Set<Integer> supportedTypes = _settings.getSupportedTypes ();
                        if ( supportedTypes.size ( ) == 0 ||
                                supportedTypes.contains ( msg.documentType ) ) {
                            // преобразуем в из TLV в JSON
                            String jsonRecord = getAsJsonMessage ( msg );
                            JsonObject jsObj = gson.fromJson ( jsonRecord, JsonObject.class );
                            // Сохраняем все прочитанные документы во временный буфер
                            _buffer.add ( new KktDocument ( msg.internalId, msg.documentType, jsObj, msg.receiveTime, msg ) );
                        }
                    } catch (KafkaTlvLibraryException|JsonParseException e) { // Пришедший документ не удалось распарсить
                        MapMessage msgMap = new MapMessage ( );
                        if ( msg!=null && msg.internalId != null ) {
                            msgMap.put ( "internalId", msg.internalId );
                            if (e.getMessage () != null && !e.getMessage ().isEmpty ()) {
                                msgMap.put ( "message", e.getMessage ( ) );
                            }
                            logger.error ( msgMap, e );
                        } else {
                            logger.error ( "internalId неизвестен", e );
                        }
                    }
                }
                // Пробуем отправить в приемник
                if ( _buffer.size ( ) > 0 ) {
                    try {
                        logger.debug ( "Sent to writer: " + _buffer.size ( ) );
                        _destination.write ( _buffer ); // Сохранение блока документов
                        // Сдвигаем оффсеты
                        commitOffsets();
                    } finally {
                        // Очищаем буфер
                        _buffer.clear ( );
                    }
                }
                // Сон
                if ( prevCount < _settings.getSleepLimit () && _settings.getSleepTimeout () != 0 ){
                    logger.info ( "Sleeping timeout..." );
                    Thread.sleep ( sleepTimeout );
                    if (sleepTimeout!=_settings.getSleepTimeout ())
                        sleepTimeout = _settings.getSleepTimeout ();
                }
            }
        }
        catch (WakeupException e) {
            if (!cancellationToken.getState ()) throw e;
        }
        catch (Exception e) {
            e.printStackTrace ();
            cancellationToken.setLastException ( e );
            logger.fatal("Фатальная ошибка в потоке: " + e.getMessage(), e);
            // Закрываем все потоки
            cancellationToken.closeAll ();
        }
        finally {
            try {
                if ( _buffer.size ( ) > 0 ) {
                    try {
                        logger.debug ( "Попытка отправки буфера " + _buffer.size ( ) );
                        _destination.write ( _buffer ); // Сохранение блока документов
                        // Сдвигаем оффсеты
                        commitOffsets ( );
                    } catch (Exception e1) {
                        e1.printStackTrace ( );
                        logger.error ( e1 );
                    }
                    // Очищаем буфер
                    _buffer.clear ( );
                }
                if (_offsetStorage!=null) _offsetStorage.close ( );
                logger.info ( "Завершение потока..." );
                if (consumer!=null) {
                    consumer.close ( );
                    consumer.wakeup ( );
                }
                if (_destination!=null){
                    _destination.close ( );
                }
            }
            catch (Exception ex){
                logger.info ( ex );
            }
            finally {
                _latch.countDown ();
            }
        }
    }

    public void shutdown() {
        cancellationToken.closeAll ();
        consumer.wakeup();
    }

    // Сохранение оффсетов, связанных с текущим консьюмером
    private void commitOffsets() throws OffsetStorageException {
        if (_settings.getOffsetStorageType () != KafkaOffsetStorageType.DEFAULT ){
            // Сохраняем во внешниее хранилище
            Set <TopicPartition> parts = consumer.assignment ();
            for (TopicPartition p : parts){
                _offsetStorage.commitPosition ( p,consumer.position ( p ) );
            }
        }else {
            // Используем стандартный механизм хранения оффсетов
            consumer.commitSync ( );
        }
    }

    private IKafkaOffsetStorage createOffsetStorage () throws IOException, InterruptedException {
        switch (_settings.getOffsetStorageType ()){
            case EXTERNAL_ZOOKEEPER:
                if ( _offsetStorage == null ){
                    ZookeeperKafkaStorage zkStore = new ZookeeperKafkaStorage (
                            _settings.getZookeeperServers (),
                            _settings.getZookeeperOffsetsPath () );
                    zkStore.connect ();
                    _offsetStorage = zkStore;
                }
                break;
        }
        return _offsetStorage;
    }

    // Преобразовывает запись из Кафки в формат Json
    private String getAsJsonMessage(DecryptedMessage msg) throws KafkaTlvLibraryException{
        try{
            String jsonStr = TlvLibrary.transformToJson(msg.tlv,msg.internalId,msg.receiveTime);
            if ( jsonStr.charAt(0) != '{' ){
                throw new KafkaTlvLibraryException(msg.internalId + '\t' + jsonStr);
            }
            return jsonStr;
        }
        catch(Exception ex){
            throw new KafkaTlvLibraryException(ex.getMessage ());
        }
    }
}
