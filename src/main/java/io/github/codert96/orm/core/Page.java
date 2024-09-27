package io.github.codert96.orm.core;

import lombok.Data;

import java.io.Serial;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

@Data
public class Page<T> implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;

    protected long current = 1;
    protected long size = 10;

    protected long total = 0;

    protected List<T> records = new ArrayList<>();


    public static <T> Page<T> of(long current, long size) {
        Page<T> page = new Page<>();
        page.setCurrent(current);
        page.setSize(size);
        return page;
    }
}
