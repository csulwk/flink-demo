package com.flink.util;

import cn.hutool.core.collection.CollectionUtil;
import cn.hutool.core.lang.Assert;
import com.flink.config.Constant;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * 解析配置文件
 * @author kai
 * @date 2021-03-07 10:15
 */
public class PropertyUtils {
    private static Logger LOG = LoggerFactory.getLogger(PropertyUtils.class);


    /**
     * 读取配置文件
     * @param filePath filePath
     * @return Properties
     */
    public static Properties readProps(String filePath) {
        Assert.notBlank(filePath, "配置文件路径为空！");
        Properties props = new Properties();
        InputStream in = null;
        try {
            File file = new File(filePath);
            if (file.canRead()) {
                LOG.info("FILE...");
                // 直接读取文件并修复中文乱码问题
                in = new BufferedInputStream(new FileInputStream(file));
                BufferedReader bf = new BufferedReader(new  InputStreamReader(in, StandardCharsets.UTF_8));
                props.load(bf);

            } else {
                LOG.info("PATH...");
                // 从当前路径中获取文件流
                ClassLoader classLoader = PropertyUtils.class.getClassLoader();
                in = classLoader.getResourceAsStream(filePath);
                props.load(in);
            }
        } catch (IOException e) {
            LOG.error(e.getMessage(), e);
        } finally {
            try {
                if (in != null) {
                    in.close();
                }
            } catch (IOException e) {
                LOG.error(e.getMessage(), e);
            }
        }
        return formatProps(props);
    }

    /**
     * 移除配置中的首尾引号
     * @param props props
     * @return props
     */
    private static Properties formatProps(Properties props) {
        final String quotation = "\"";
        Properties fProps = new Properties();
        props.forEach((k,v) -> {
            String fv = StringUtils.removeEnd(StringUtils.removeStart(v.toString(), quotation), quotation);
            fProps.put(k.toString(), fv);
        });
        return fProps;
    }

    /**
     * 打印配置信息
     */
    public static void print(Properties props) {
        props.forEach((k,v) -> {
            LOG.info("{}: {} = {}", Constant.PROP_FILE_PATH, k, v);
        });
    }
}
