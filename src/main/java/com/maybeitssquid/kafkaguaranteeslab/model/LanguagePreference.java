package com.maybeitssquid.kafkaguaranteeslab.model;

import java.util.Locale;

public record LanguagePreference(String customerId, Locale preferredLanguage) {}
