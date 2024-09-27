package io.github.codert96.orm.core;

import lombok.Data;
import org.springframework.util.StringUtils;

@Data
public class ColumnInfo {

    private String tableName;

    private String fieldName;

    private String columnName;

    private boolean insertable = true;

    private boolean updatable = true;

    public String fullName() {
        StringBuilder stringBuilder = new StringBuilder();
        if (StringUtils.hasText(tableName)) {
            stringBuilder.append(tableName).append(".");
        }
        stringBuilder.append(columnName);
        return stringBuilder.toString();
    }
}
