package pl.mm;

import com.opencsv.CSVReader;
import io.confluent.ksql.api.client.AcksPublisher;
import io.confluent.ksql.api.client.Client;
import io.confluent.ksql.api.client.KsqlArray;
import io.confluent.ksql.api.client.KsqlObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.log4j.BasicConfigurator;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Signal;

import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

@Slf4j
public class Up {

    private static final int BATCH_MAX_SIZE = 200;
    private static final Client client = Client.create(CommonConfig.OPTIONS);

    public static void main(String[] args) {
        BasicConfigurator.configure();
        Mono.justOrEmpty(Up.class.getClassLoader().getResourceAsStream("sample.csv"))
                .map(InputStreamReader::new)
                .flatMapMany(Up::streamCsvRowsFromFile)
                .map(Arrays::asList)
                .switchOnFirst(Up::extractKeysFromHeader)
                .window(BATCH_MAX_SIZE)
                .flatMap(Up::streamInserts)
                .flatMap(Function.identity())
                .doOnNext(ack -> log.info("===> ack: {}", ack))
                .count()
                .doOnNext(count -> log.info("count: {}", count))
                .block();
        client.close();
    }

    private static Publisher<String[]> streamCsvRowsFromFile(Reader reader) {
        return Flux.using(
                () -> new CSVReader(reader),
                Flux::fromIterable);
    }

    private static Publisher<KsqlObject> extractKeysFromHeader(
            Signal<? extends List<String>> signal, Flux<List<String>> csvRowValues) {
        return Mono.fromSupplier(signal)
                .flatMapMany(keys -> csvRowValues.skip(1L)
                        .map(KsqlArray::new)
                        .map(values -> KsqlObject.fromArray(keys, values)));
    }

    private static Publisher<AcksPublisher> streamInserts(Flux<KsqlObject> insertsPublisher) {
        return Mono.fromFuture(client.streamInserts("SAMPLE_STREAM", insertsPublisher));
    }
}
