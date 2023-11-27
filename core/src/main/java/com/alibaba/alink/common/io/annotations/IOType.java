package com.alibaba.alink.common.io.annotations;

/**
 * Type of source&&sink operator.
 *
 * source sink操作类型
 *
 */
public enum IOType {
	/**
	 * Batch source operator.
	 */
	SourceBatch,

	/**
	 * Batch sink operator.
	 */
	SinkBatch,

	/**
	 * Stream source operator.
	 */
	SourceStream,

	/**
	 * Stream sink operator.
	 */
	SinkStream,

	/**
	 * local source operator.
	 */
	SourceLocal,

	/**
	 * Stream sink operator.
	 */
	SinkLocal,

}
