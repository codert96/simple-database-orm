package io.github.codert96.orm.core;

import java.io.Serializable;
import java.util.function.Function;

public interface ColumnFunction<DTO, R> extends Function<DTO, R>, Serializable {
}
