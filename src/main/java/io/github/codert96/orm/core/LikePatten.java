package io.github.codert96.orm.core;

import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public enum LikePatten {
    /**
     * 右边是%
     */
    RIGHT("CONCAT(:%s, '%%')"),

    /**
     * 左边是%
     */
    LEFT("CONCAT('%%', :%s)"),
    /**
     * 两边都有%
     */
    LIKE("CONCAT('%%', :%s, '%%')");

    public final String patten;
}
