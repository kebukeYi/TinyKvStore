package com.xiaohansong.kvstore.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

/**
 * 位置信息
 */
@Data
@AllArgsConstructor
@Builder
public class Position {

    /**
     * 稀疏索引名字
     */
    private String PartDataName;

    /**
     * 开始
     */
    private long start;

    /**
     * 长度
     */
    private long len;
}
