package com.maybeitssquid.kafkaguaranteeslab.lab;

import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * Turns the circuit breakers off when {@code lab.circuit-breakers.enabled} is false, for the stages
 * of the lab that run without Resilience4j. A disabled circuit breaker lets every call through and
 * never opens.
 *
 * <p>The retries are turned off separately, by setting their {@code max-attempts} to 1.
 */
@Component
public class CircuitBreakerSwitch implements SmartInitializingSingleton {

  private static final String[] CIRCUIT_BREAKERS = {
    "languagePreferenceProducer", "languagePreferenceConsumer"
  };

  private final CircuitBreakerRegistry registry;
  private final boolean enabled;

  /**
   * Creates the switch.
   *
   * @param registry the registry holding the application's circuit breakers
   * @param enabled whether the circuit breakers should stay active
   */
  public CircuitBreakerSwitch(
      CircuitBreakerRegistry registry, @Value("${lab.circuit-breakers.enabled}") boolean enabled) {
    this.registry = registry;
    this.enabled = enabled;
  }

  @Override
  public void afterSingletonsInstantiated() {
    if (!enabled) {
      for (String name : CIRCUIT_BREAKERS) {
        registry.circuitBreaker(name).transitionToDisabledState();
      }
    }
  }
}
