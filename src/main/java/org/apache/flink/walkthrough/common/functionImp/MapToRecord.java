package org.apache.flink.walkthrough.common.functionImp;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.walkthrough.common.entity.Record;
import org.apache.flink.walkthrough.common.entity.RecordImp;
import org.apache.flink.walkthrough.common.util.Util;

/**
 * @author zhang lianhui
 * @date 2020/10/29 3:30 下午
 */
public class MapToRecord implements MapFunction<String, Record> {
	@Override
	public Record map(String json) throws Exception {
		return Util.json2obj(json, RecordImp.class);
	}
}
