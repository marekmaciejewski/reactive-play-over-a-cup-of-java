package pl.mm;

import io.confluent.ksql.api.client.ClientOptions;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.Properties;

@Slf4j
@UtilityClass
class CommonConfig {

    private final Properties properties = new Properties();

    static {
        try (InputStream propertiesInputStream = CommonConfig.class.getClassLoader().getResourceAsStream("app.properties")) {
            properties.load(propertiesInputStream);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    final ClientOptions OPTIONS = ClientOptions.create()
            .setBasicAuthCredentials(properties.getProperty("key"), properties.getProperty("password"))
            .setHost(properties.getProperty("host"))
            .setPort(443)
            .setUseTls(true)
            .setUseAlpn(true);
}
/*
SELECT COUNT(*) FROM SAMPLE_STREAM EMIT CHANGES;
*/
