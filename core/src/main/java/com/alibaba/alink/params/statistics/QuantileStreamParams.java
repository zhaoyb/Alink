package com.alibaba.alink.params.statistics;

import com.alibaba.alink.params.shared.HasTimeCol_null;

public interface QuantileStreamParams<T> extends
	StatBaseParams <T>,
	HasTimeCol_null <T>,
	HasDalayTime <T>,
	HasQuantileNum <T> {

}
