package com.maybeitssquid.kafkaguaranteeslab;

import static org.assertj.core.api.Assertions.assertThat;

import com.maybeitssquid.kafkaguaranteeslab.model.LanguagePreference;
import com.maybeitssquid.kafkaguaranteeslab.producer.LanguagePreferenceProducer;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;

@SpringBootTest
@DirtiesContext
@EmbeddedKafka(
    partitions = 1,
    topics = {LanguagePreferenceProducer.TOPIC})
class LanguagePreferenceProducerTest {

  @Autowired private LanguagePreferenceProducer producer;

  @Test
  void publish_sendsEventToTopic() throws Exception {
    var event = new LanguagePreference("cust-1", Locale.FRENCH);

    var future = producer.publish(event);

    var result = future.get(5, TimeUnit.SECONDS);
    assertThat(result.getRecordMetadata().topic()).isEqualTo(LanguagePreferenceProducer.TOPIC);
    assertThat(result.getRecordMetadata().offset()).isGreaterThanOrEqualTo(0);
  }
}
