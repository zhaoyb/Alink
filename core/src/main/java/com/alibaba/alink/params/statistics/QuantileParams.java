package com.alibaba.alink.params.statistics;

import com.alibaba.alink.params.shared.colname.HasSelectedCols;

public interface QuantileParams<T> extends
	HasSelectedCols <T>,
	HasQuantileNum <T>,
	HasRoundMode <T> {
}
