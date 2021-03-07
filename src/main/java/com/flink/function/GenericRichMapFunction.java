package com.flink.function;

import cn.hutool.core.map.CaseInsensitiveMap;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.functions.RichMapFunction;

/**
 * 不区分大小写的集合
 * @author kai
 * @date 2021-03-04 21:24
 */
public class GenericRichMapFunction extends RichMapFunction<GenericRecord, CaseInsensitiveMap> {
    @Override
    public CaseInsensitiveMap map(GenericRecord genericRecord) throws Exception {
        return JSONObject.parseObject(genericRecord.toString(), new TypeReference<CaseInsensitiveMap>(){});
    }
}
