package io.github.codert96.orm.core;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.TypeFactory;
import io.github.codert96.orm.config.Configuration;
import io.github.codert96.orm.utils.Utils;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.ColumnMapRowMapper;
import org.springframework.jdbc.core.namedparam.BeanPropertySqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations;
import org.springframework.lang.NonNull;
import org.springframework.util.StopWatch;

import java.io.Serial;
import java.io.Serializable;
import java.text.MessageFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Stream;

@Slf4j
@SuppressWarnings({"unused", "UnusedReturnValue"})
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public class Example<DTO, T> implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;

    @Setter
    private static NamedParameterJdbcOperations namedParameterJdbcOperations;

    @Setter
    private static ObjectMapper objectMapper;

    private final DTO dto;
    private final Class<T> resultClass;
    private final List<String> firstExpressions = new ArrayList<>();

    private final List<String> selectExpressions = new ArrayList<>();

    private final List<String> whereExpressions = new ArrayList<>();

    private final List<String> lastExpressions = new ArrayList<>();

    @Setter
    @Accessors(chain = true)
    private boolean useBefore = true;

    @Setter
    @Accessors(chain = true)
    private boolean useAfter = true;

    @Setter(AccessLevel.PRIVATE)
    @Accessors(chain = true)
    private String tableName;
    private final List<Consumer<Example<DTO, ?>>> beforeQuery = new ArrayList<>();
    private final List<Consumer<List<?>>> afterQuery = new ArrayList<>();


    public static <DTO, T> Example<DTO, T> of(@NonNull DTO dto, @NonNull Class<T> entityClass) {
        Utils.extract(entityClass);
        return new Example<>(dto, entityClass).setTableName(Utils.extractTableName(entityClass));
    }

    public final Example<DTO, T> first(String... first) {
        firstExpressions.addAll(Arrays.asList(first));
        return this;
    }

    public final Example<DTO, T> last(String... last) {
        lastExpressions.addAll(Arrays.asList(last));
        return this;
    }

    public final Example<DTO, T> select(String... columns) {
        selectExpressions.addAll(Arrays.asList(columns));
        return this;
    }

    @SafeVarargs
    public final <R> Example<DTO, T> select(ColumnFunction<DTO, R>... columns) {
        selectExpressions.addAll(Stream.of(columns).map(Utils::extractColumn).map(ColumnInfo::getColumnName).toList());
        return this;
    }

    @SafeVarargs
    public final <R> Example<DTO, T> eq(boolean ignoreNull, ColumnFunction<DTO, R>... columns) {
        return operator(ignoreNull, "=", columns);
    }

    @SafeVarargs
    public final <R> Example<DTO, T> ne(boolean ignoreNull, ColumnFunction<DTO, R>... columns) {
        return operator(ignoreNull, "<>", columns);
    }

    @SafeVarargs
    public final <R> Example<DTO, T> le(boolean ignoreNull, ColumnFunction<DTO, R>... columns) {
        return operator(ignoreNull, "<=", columns);
    }

    @SafeVarargs
    public final <R> Example<DTO, T> ge(boolean ignoreNull, ColumnFunction<DTO, R>... columns) {
        return operator(ignoreNull, ">=", columns);
    }

    @SafeVarargs
    public final <R> Example<DTO, T> lt(boolean ignoreNull, ColumnFunction<DTO, R>... columns) {
        return operator(ignoreNull, "<", columns);
    }

    @SafeVarargs
    public final <R> Example<DTO, T> gt(boolean ignoreNull, ColumnFunction<DTO, R>... columns) {
        return operator(ignoreNull, ">", columns);
    }

    @SafeVarargs
    public final <R> Example<DTO, T> eq(ColumnFunction<DTO, R>... columns) {
        return operator(false, "=", columns);
    }

    @SafeVarargs
    public final <R> Example<DTO, T> ne(ColumnFunction<DTO, R>... columns) {
        return operator(false, "<>", columns);
    }

    @SafeVarargs
    public final <R> Example<DTO, T> le(ColumnFunction<DTO, R>... columns) {
        return operator(false, "<=", columns);
    }

    @SafeVarargs
    public final <R> Example<DTO, T> ge(ColumnFunction<DTO, R>... columns) {
        return operator(false, ">=", columns);
    }

    @SafeVarargs
    public final <R> Example<DTO, T> lt(ColumnFunction<DTO, R>... columns) {
        return operator(false, "<", columns);
    }

    @SafeVarargs
    public final <R> Example<DTO, T> gt(ColumnFunction<DTO, R>... columns) {
        return operator(false, ">", columns);
    }

    @SafeVarargs
    public final <R> Example<DTO, T> in(boolean ignoreNull, ColumnFunction<DTO, R>... columns) {
        for (ColumnFunction<DTO, R> column : columns) {
            if (ignoreNull && Utils.isIgnore(dto, column)) {
                continue;
            }
            ColumnInfo columnInfo = Utils.extractColumn(column);
            whereExpressions.add("%s IN (:%s)".formatted(columnInfo.getColumnName(), columnInfo.getFieldName()));
        }
        return this;
    }

    @SafeVarargs
    public final <R> Example<DTO, T> in(ColumnFunction<DTO, R>... columns) {
        return in(false, columns);
    }

    @SafeVarargs
    public final <R> Example<DTO, T> notIn(boolean ignoreNull, ColumnFunction<DTO, R>... columns) {
        for (ColumnFunction<DTO, R> column : columns) {
            if (ignoreNull && Utils.isIgnore(dto, column)) {
                continue;
            }
            ColumnInfo columnInfo = Utils.extractColumn(column);
            whereExpressions.add("%s NOT IN (:%s)".formatted(columnInfo.getColumnName(), columnInfo.getFieldName()));
        }
        return this;
    }

    @SafeVarargs
    public final <R> Example<DTO, T> isNull(ColumnFunction<DTO, R>... columns) {
        for (ColumnFunction<DTO, R> column : columns) {
            ColumnInfo columnInfo = Utils.extractColumn(column);
            whereExpressions.add("%s IS NULL".formatted(columnInfo.getColumnName()));
        }
        return this;
    }

    @SafeVarargs
    public final <R> Example<DTO, T> isNotNull(ColumnFunction<DTO, R>... columns) {
        for (ColumnFunction<DTO, R> column : columns) {
            ColumnInfo columnInfo = Utils.extractColumn(column);
            whereExpressions.add("%s IS NOT NULL".formatted(columnInfo.getColumnName()));
        }
        return this;
    }

    @SafeVarargs
    public final <R> Example<DTO, T> notIn(ColumnFunction<DTO, R>... columns) {
        return notIn(false, columns);
    }

    public final <R> Example<DTO, T> notBetween(ColumnFunction<DTO, R> column, ColumnFunction<DTO, R> first, ColumnFunction<DTO, R> last) {
        return notBetween(false, column, first, last);
    }

    public final <R> Example<DTO, T> notBetween(boolean ignoreNull, ColumnFunction<DTO, R> column, ColumnFunction<DTO, R> first, ColumnFunction<DTO, R> last) {
        boolean firstIgnore = Utils.isIgnore(dto, first);
        boolean lastIgnore = Utils.isIgnore(dto, last);
        ColumnInfo columnInfo = Utils.extractColumn(column);
        if (ignoreNull && firstIgnore && lastIgnore) {
            return this;
        }
        if (!firstIgnore && !lastIgnore) {
            return apply("%s NOT BETWEEN {0} AND {1}".formatted(columnInfo.getColumnName()), first, last);
        }
        if (!firstIgnore) {
            return apply("%s < {0}".formatted(columnInfo.getColumnName()), first);
        }

        if (!lastIgnore) {
            return apply("%s > {0}".formatted(columnInfo.getColumnName()), last);
        }
        return this;
    }

    public final <R> Example<DTO, T> between(ColumnFunction<DTO, R> column, ColumnFunction<DTO, R> first, ColumnFunction<DTO, R> last) {
        return between(false, column, first, last);
    }

    public final <R> Example<DTO, T> between(boolean ignoreNull, ColumnFunction<DTO, R> column, ColumnFunction<DTO, R> first, ColumnFunction<DTO, R> last) {
        boolean firstIgnore = Utils.isIgnore(dto, first);
        boolean lastIgnore = Utils.isIgnore(dto, last);
        ColumnInfo columnInfo = Utils.extractColumn(column);
        if (ignoreNull && firstIgnore && lastIgnore) {
            return this;
        }
        if (!firstIgnore && !lastIgnore) {
            return apply("%s BETWEEN {0} AND {1}".formatted(columnInfo.getColumnName()), first, last);
        }
        if (!firstIgnore) {
            return apply("%s >= {0}".formatted(columnInfo.getColumnName()), first);
        }

        if (!lastIgnore) {
            return apply("%s <= {0}".formatted(columnInfo.getColumnName()), last);
        }
        return this;

    }

    public final Example<DTO, T> or(Consumer<Example<DTO, T>> consumer) {
        return apply("OR", consumer);
    }

    public final Example<DTO, T> and(Consumer<Example<DTO, T>> consumer) {
        return apply("AND", consumer);
    }

    private Example<DTO, T> apply(String str, Consumer<Example<DTO, T>> consumer) {
        Example<DTO, T> example = new Example<>(dto, resultClass);
        consumer.accept(example);
        String where = example.toWhere();
        if (!example.whereExpressions.isEmpty()) {
            if (example.whereExpressions.size() > 1) {
                whereExpressions.add(
                        " %s (%s)".formatted(str, where)
                );
            } else {
                whereExpressions.add(
                        " %s %s".formatted(str, where)
                );
            }
        }
        return this;
    }

    @SafeVarargs
    public final <R> Example<DTO, T> like(ColumnFunction<DTO, R>... columns) {
        return like(false, false, LikePatten.LIKE, columns);
    }

    @SafeVarargs
    public final <R> Example<DTO, T> like(boolean notLike, LikePatten patten, ColumnFunction<DTO, R>... columns) {
        return like(false, notLike, patten, columns);
    }

    @SafeVarargs
    public final <R> Example<DTO, T> like(boolean ignoreNull, boolean notLike, LikePatten patten, ColumnFunction<DTO, R>... columns) {
        for (ColumnFunction<DTO, R> column : columns) {
            if (ignoreNull && Utils.isIgnore(dto, column)) {
                continue;
            }
            ColumnInfo columnInfo = Utils.extractColumn(column);
            whereExpressions.add(
                    "%s %sLIKE ".concat(patten.patten).formatted(
                            columnInfo.getColumnName(),
                            notLike ? "NOT " : "",
                            columnInfo.getFieldName()
                    )
            );
        }
        return this;
    }


    @SafeVarargs
    public final <R> Example<DTO, T> apply(String sqlCorn, ColumnFunction<DTO, R>... columns) {
        Object[] array = Arrays.stream(columns).map(Utils::extractColumn).map(columnInfo -> ":%s".formatted(columnInfo.getFieldName())).toArray();
        whereExpressions.add(
                MessageFormat.format(sqlCorn, array)
        );
        return this;
    }

    @SafeVarargs
    private <R> Example<DTO, T> operator(boolean ignoreNull, String operator, ColumnFunction<DTO, R>... columns) {
        MessageFormat format = new MessageFormat("{0} {1} :{2}");
        for (ColumnFunction<DTO, ?> column : columns) {
            if (ignoreNull && Utils.isIgnore(dto, column)) {
                continue;
            }
            ColumnInfo columnInfo = Utils.extractColumn(column);
            whereExpressions.add(format.format(new Object[]{columnInfo.getColumnName(), operator, columnInfo.getFieldName()}));
        }
        return this;
    }


    public Example<DTO, T> beforeQuery(Consumer<Example<DTO, ?>> consumer) {
        beforeQuery.add(consumer);
        return this;
    }

    public Example<DTO, T> afterQuery(Consumer<List<?>> consumer) {
        afterQuery.add(consumer);
        return this;
    }


    public Example<DTO, T> clearFirst() {
        firstExpressions.clear();
        return this;
    }

    public Example<DTO, T> clearLast() {
        lastExpressions.clear();
        return this;
    }

    public Example<DTO, T> clearSelect() {
        selectExpressions.clear();
        return this;
    }

    public Example<DTO, T> clearWhere() {
        whereExpressions.clear();
        return this;
    }

    public Example<DTO, T> clearBefore() {
        beforeQuery.clear();
        return this;
    }

    public Example<DTO, T> clearAfter() {
        lastExpressions.clear();
        return this;
    }

    private String toWhere() {
        StringBuilder stringBuilder = new StringBuilder();
        for (int i = 0; i < whereExpressions.size(); i++) {
            String s = whereExpressions.get(i);
            if (s.startsWith(" AND ") || s.startsWith(" OR ")) {
                if (i == 0) {
                    stringBuilder.append(
                            s.replaceFirst("^ AND |^ OR ", "")
                    );
                } else {
                    String string = stringBuilder.toString();
                    stringBuilder.setLength(0);
                    stringBuilder.append("(%s)".formatted(string)).append(s);
                }
            } else if (i != 0) {
                stringBuilder.append(" AND ").append(s);
            } else {
                stringBuilder.append(s);
            }

        }
        return stringBuilder.toString().trim();
    }

    public Long count() {
        return count("1");
    }

    public Long count(String column) {
        //noinspection unchecked
        Class<Map<String, Long>> rawClass = (Class<Map<String, Long>>) TypeFactory.defaultInstance()
                .constructMapType(HashMap.class, String.class, Long.class)
                .getRawClass();
        Example<DTO, Map<String, Long>> example = copy(rawClass);
        List<Map<String, Long>> query = example.clearSelect().select("COUNT(%s) AS count_number".formatted(column)).list();
        if (query.isEmpty()) {
            return 0L;
        }
        Map<String, Long> map = query.get(0);
        return map.getOrDefault("count_number", 0L);
    }

    public Page<T> page(Page<T> page) {
        Long count = copy()
                .setUseAfter(false)
                .count("1");
        page.setTotal(count);
        if (Objects.equals(0L, count)) {
            return page;
        }
        long offset = (page.getCurrent() - 1) * page.getSize();
        last("LIMIT %s OFFSET %s".formatted(page.getSize(), offset));
        List<T> query = list();
        page.setRecords(query);
        return page;
    }

    public List<T> list() {
        if (useBefore) {
            Configuration.BEFORE_QUERY.forEach(consumer -> consumer.accept(this, resultClass));
            beforeQuery.forEach(consumer -> consumer.accept(this));
        }
        StringJoiner execSql = new StringJoiner(" ");
        if (!firstExpressions.isEmpty()) {
            execSql.add(
                    String.join(System.lineSeparator(), firstExpressions)
            );
        }
        execSql.add("SELECT");
        if (selectExpressions.isEmpty()) {
            List<String> extract = Utils.extract(resultClass);
            if (extract.isEmpty()) {
                execSql.add("*");
            } else {
                execSql.add(String.join(",", extract));
            }
        } else {
            execSql.add(
                    String.join(System.lineSeparator(), selectExpressions)
            );
        }
        execSql.add("FROM").add(tableName);
        if (!whereExpressions.isEmpty()) {
            execSql.add("WHERE").add(toWhere());
        }
        if (!lastExpressions.isEmpty()) {
            execSql.add(
                    String.join(System.lineSeparator(), lastExpressions)
            );
        }
        String sql = execSql.toString();
        StopWatch stopWatch = new StopWatch("查询：%s".formatted(tableName));
        stopWatch.start("执行SQL");
        List<Map<String, Object>> list = namedParameterJdbcOperations.query(sql, new BeanPropertySqlParameterSource(dto), new LowerCaseColumnMapRowMapper());
        if (list.isEmpty()) {
            return new ArrayList<>();
        }
        ArrayList<T> result = objectMapper.convertValue(
                list,
                TypeFactory.defaultInstance().constructCollectionType(ArrayList.class, resultClass)
        );
        if (useAfter) {
            Configuration.AFTER_QUERY.forEach(consumer -> consumer.accept(result, resultClass));
            afterQuery.forEach(consumer -> consumer.accept(result));
        }

        stopWatch.stop();
        StringJoiner formatLog = new StringJoiner(System.lineSeparator());

        formatLog.add("")
                .add(stopWatch.prettyPrint(TimeUnit.SECONDS).concat("-".repeat(42)))
                .add(Utils.formatSql(sql, dto))
                .add("-".repeat(42))
                .add("query result size: " + result.size())
                .add("-".repeat(42))
        ;
        log.debug(formatLog.toString());
        return result;
    }

    public static class LowerCaseColumnMapRowMapper extends ColumnMapRowMapper {
        @Override
        protected String getColumnKey(String columnName) {
            return columnName.toLowerCase();
        }
    }

    public Example<DTO, T> copy() {
        return copy(resultClass);
    }

    public <R> Example<DTO, R> copy(Class<R> resultClass) {
        Example<DTO, R> copy = new Example<>(dto, resultClass);
        copy.firstExpressions.addAll(this.firstExpressions);
        copy.lastExpressions.addAll(this.lastExpressions);
        copy.selectExpressions.addAll(this.selectExpressions);
        copy.whereExpressions.addAll(this.whereExpressions);
        copy.beforeQuery.addAll(beforeQuery);
        copy.afterQuery.addAll(afterQuery);
        copy.useBefore = this.useBefore;
        copy.useAfter = this.useAfter;
        copy.tableName = tableName;
        return copy;
    }


    @Override
    public String toString() {
        return toWhere();
    }
}
