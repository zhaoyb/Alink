package com.alibaba.alink.params.outlier;

/**
 * 算法相关: 需要设置聚类距离限制(epsilon)、规模限制(minPoints)、距离类型(distanceType) 数据转换相关: 需要指定列名（featureCols）和MTable名（selectedCol）
 * 使用相关：epsilon不确定时可以设为-1，将自动选择合适的值
 *
 * @param <T>
 */
public interface DbscanDetectorParams<T> extends DbscanAlgoParams <T>, WithMultiVarParams <T> {
}