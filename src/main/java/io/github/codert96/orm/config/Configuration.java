package io.github.codert96.orm.config;

import io.github.codert96.orm.core.Example;
import lombok.Data;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;

@Data
public class Configuration {
    public static final List<BiConsumer<List<?>, Class<?>>> AFTER_QUERY = new ArrayList<>();

    public static final List<BiConsumer<Example<?, ?>, Class<?>>> BEFORE_QUERY = new ArrayList<>();
}
