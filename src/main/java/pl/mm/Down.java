package pl.mm;

import io.confluent.ksql.api.client.Client;
import io.confluent.ksql.api.client.Row;
import io.smallrye.mutiny.Uni;
import lombok.extern.slf4j.Slf4j;
import org.apache.log4j.BasicConfigurator;
import reactor.adapter.JdkFlowAdapter;

import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
public class Down {

    private static final Map<String, Object> PROPERTIES = Map.of("auto.offset.reset", "latest");
    private static final Client client = Client.create(CommonConfig.OPTIONS);

    public static void main(String[] args) {
        BasicConfigurator.configure();
        String sql = "SELECT * FROM SAMPLE_STREAM EMIT CHANGES;";
        Uni.createFrom().future(client.streamQuery(sql, PROPERTIES))
                .invoke(streamedQueryResult -> log.info("===> queryId: {}", streamedQueryResult.queryID()))
                .onItem().transformToMulti(JdkFlowAdapter::publisherToFlowPublisher)
//                .onItem().transformToMulti(AdaptersToFlow::publisher)
                .map(Row::asObject)
                .onItem().invoke(row -> log.info("===> row: {}", row))
                .onCompletion().invoke(client::close)
                .collect().with(Collectors.counting())
                .subscribe().with(count -> log.info("count: {}", count));
    }
}
