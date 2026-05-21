package com.example.atleastonce.producer;

import com.example.atleastonce.model.LanguagePreference;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/language-preferences")
public class LanguagePreferenceController {

  private final LanguagePreferenceProducer producer;

  public LanguagePreferenceController(LanguagePreferenceProducer producer) {
    this.producer = producer;
  }

  @PostMapping
  public ResponseEntity<String> publish(@RequestBody LanguagePreference event) {
    producer.publish(event);
    return ResponseEntity.accepted().body("Event queued: " + event.customerId());
  }
}
