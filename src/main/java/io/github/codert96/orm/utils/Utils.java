package io.github.codert96.orm.utils;

import io.github.codert96.orm.core.ColumnFunction;
import io.github.codert96.orm.core.ColumnInfo;
import jakarta.persistence.Column;
import jakarta.persistence.Table;
import jakarta.persistence.Transient;
import lombok.SneakyThrows;
import lombok.experimental.UtilityClass;
import org.springframework.cglib.core.ReflectUtils;
import org.springframework.jdbc.support.JdbcUtils;
import org.springframework.util.ClassUtils;
import org.springframework.util.ReflectionUtils;
import org.springframework.util.StringUtils;

import java.beans.PropertyDescriptor;
import java.lang.invoke.SerializedLambda;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Stream;

import static java.beans.Introspector.decapitalize;

@SuppressWarnings("unused")
@UtilityClass
public class Utils {
    private static final Map<String, Map<String, ColumnInfo>> classCache = new ConcurrentHashMap<>();
    private static final Map<Class<?>, String> tableNameCache = new ConcurrentHashMap<>();

    @SneakyThrows
    public <DTO> SerializedLambda extract(ColumnFunction<DTO, ?> function) {
        Method writeReplace = function.getClass().getDeclaredMethod("writeReplace");
        writeReplace.setAccessible(true);
        return (SerializedLambda) writeReplace.invoke(function);
    }

    public String extractFieldName(SerializedLambda lambda) {
        return extractFieldName(lambda.getImplMethodName());
    }

    public String extractFieldName(String methodName) {
        if (methodName.startsWith("get") || methodName.startsWith("set")) {
            return decapitalize(methodName.substring(3));
        } else if (methodName.startsWith("is")) {
            return decapitalize(methodName.substring(2));
        }
        throw new IllegalArgumentException("无法解析方法名: %s".formatted(methodName));
    }

    @SneakyThrows
    public <DTO> ColumnInfo extractColumn(ColumnFunction<DTO, ?> function) {
        SerializedLambda serializedLambda = extract(function);
        return cache(ClassUtils.forName(serializedLambda.getImplClass().replace("/", "."), null), serializedLambda.getImplMethodName());
    }

    private ColumnInfo cache(Class<?> clazz, String method) {
        String extractedTableName = extractTableName(clazz);
        return classCache.computeIfAbsent(clazz.getName(), className -> Collections.synchronizedMap(new LinkedHashMap<>()))
                .computeIfAbsent(method, methodName -> {
                    ColumnInfo columnInfo = new ColumnInfo();
                    String fieldName = extractFieldName(methodName);
                    Field declaredField = ReflectionUtils.findField(clazz, fieldName);
                    if (Objects.isNull(declaredField)) {
                        return null;
                    }
                    columnInfo.setFieldName(fieldName);
                    columnInfo.setColumnName(JdbcUtils.convertPropertyNameToUnderscoreName(fieldName));
                    columnInfo.setTableName(extractedTableName);
                    ReflectionUtils.makeAccessible(declaredField);
                    if (declaredField.isAnnotationPresent(Transient.class) && Modifier.isTransient(declaredField.getModifiers())) {
                        return null;
                    }
                    if (declaredField.isAnnotationPresent(Column.class)) {
                        Column column = declaredField.getAnnotation(Column.class);
                        String columnName = column.name();
                        if (!StringUtils.hasText(columnName)) {
                            columnName = JdbcUtils.convertPropertyNameToUnderscoreName(fieldName);
                        }
                        String tableName = column.table();
                        if (StringUtils.hasText(tableName)) {
                            columnInfo.setTableName(tableName);
                        }
                        columnInfo.setColumnName(columnName);
                        columnInfo.setInsertable(columnInfo.isInsertable());
                        columnInfo.setUpdatable(columnInfo.isUpdatable());
                    }
                    return columnInfo;
                });
    }

    public String extractTableName(Class<?> clazz) {
        return tableNameCache.computeIfAbsent(clazz, tmp -> {
            String simpleName = tmp.getSimpleName();
            AtomicReference<String> tableNameRef = new AtomicReference<>(
                    JdbcUtils.convertPropertyNameToUnderscoreName(simpleName)
            );
            if (clazz.isAnnotationPresent(Table.class)) {
                Table table = clazz.getAnnotation(Table.class);
                String name = table.name();
                String schema = table.schema();
                if (StringUtils.hasText(name)) {
                    tableNameRef.set(name);
                    if (StringUtils.hasText(schema)) {
                        tableNameRef.set(
                                "%s.%s".formatted(schema, name)
                        );
                    }
                }
            }
            return tableNameRef.get();
        });
    }

    public List<String> extract(Class<?> clazz) {
        String clazzName = clazz.getName();
        if (!classCache.containsKey(clazzName)) {
            PropertyDescriptor[] beanProperties = ReflectUtils.getBeanProperties(clazz);
            Stream.of(clazz.getMethods())
                    .filter(method -> Objects.equals(method.getParameterCount(), 0))
                    .map(Method::getName)
                    .filter(method -> method.startsWith("get") || method.startsWith("is"))
                    .sorted()
                    .toList()
                    .forEach(string -> cache(clazz, string));
        }
        Map<String, ColumnInfo> map = classCache.getOrDefault(clazzName, Map.of());
        List<ColumnInfo> list = new ArrayList<>();
        map.forEach((string, columnInfo) -> list.add(columnInfo));
        return list.stream().map(ColumnInfo::getColumnName).toList();
    }

    public <DTO, R> boolean isIgnore(DTO dto, ColumnFunction<DTO, R> columnFunction) {
        R apply = columnFunction.apply(dto);
        if (Objects.isNull(apply)) {
            return true;
        }
        if (apply instanceof String string) {
            return string.isEmpty();
        }
        if (apply instanceof Collection<?> collection) {
            return collection.isEmpty();
        }
        if (apply instanceof Map<?, ?> map) {
            return map.isEmpty();
        }
        return false;
    }

    public static String formatSql(String sql, Object args) {
        Class<?> clazz = args.getClass();
        Field[] declaredFields = clazz.getDeclaredFields();
        for (Field declaredField : declaredFields) {
            String fieldName = ":%s".formatted(declaredField.getName());
            if (sql.contains(fieldName)) {
                ReflectionUtils.makeAccessible(declaredField);
                Object data = ReflectionUtils.getField(declaredField, args);
                if (Objects.isNull(data)) {
                    sql = sql.replace(fieldName, "null");
                } else {
                    sql = sql.replace(fieldName, "'%s'".formatted(data));
                }
            }
        }

        return sql;
    }

    private interface Func<T, R> extends Function<T, R> {
        default R apply(T t) {
            try {
                return exec(t);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        R exec(T t) throws Exception;
    }
}
