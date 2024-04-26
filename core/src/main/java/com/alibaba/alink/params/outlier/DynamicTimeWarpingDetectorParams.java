package com.alibaba.alink.params.outlier;

public interface DynamicTimeWarpingDetectorParams<T> extends
	DynamicTimeWarpingAlgoParams <T>,
	OutlierDetectorParams <T>,
	WithUniVarParams <T> {
}
