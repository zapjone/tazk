package com.tazk.launcher;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 启动参数解析(tazk启动需要的参数配置方式)
 *
 * @author zap
 * @version 1.0, 2020/05/23
 */
public class TazkSubmitOptionParser {
    private final Pattern eqSeparatedOpt = Pattern.compile("(--[^=]+)=(.+)");

    /**
     * tazk launcher arguments.
     */
    protected final String EXECUTION_ENGINE = "--execution-engine";
    protected final String NAME = "--name";
    protected final String CONNECT = "--connect";
    protected final String USER_NAME = "--username";
    protected final String PASSWORD = "--password";
    protected final String JAR_ADDR = "--tazk-jar";

    /**
     * spark launcher arguments.
     */
    protected final String SPARK_HOME = "--spark-home";
    protected final String SPARK_MASTER = "--spark-master";
    protected final String SPARK_DEPLOY_MODE = "--spark-deploy-mode";
    protected final String SPARK_CONF = "--spark-conf";
    protected final String SPARK_PROPS_FILE = "--spark-properties-file";
    protected final String SPARK_QUEUE = "--spark-queue";
    protected final String SPARK_DRIVER_MEM = "--spark-driver-memory";
    protected final String SPARK_DRIVER_CORES = "--spark-driver-cores";
    protected final String SPARK_EXECUTOR_MEM = "--spark-executor-memory";
    protected final String SPARK_TOTAL_EXECUTOR_CORES = "--spark-total-executor-cores";
    protected final String SPARK_NUM_EXECUTORS = "--spark-num-executors";
    protected final String SPARK_EXECUTOR_CORES = "--spark-executor-cores";

    /**
     * mongo launcher arguments.
     */
    protected final String MONGO_DATABASE = "--mongo-database";
    protected final String MONGO_COLLECTION = "--mongo-collection";
    protected final String MONGO_READ_PREFERENCE = "--mongo-read-preference";
    protected final String MONGO_EXTERNAL_CONF = "--mongo-conf";
    protected final String MONGO_IMPORT_CONDITION = "--mongo-import-condition";
    protected final String MONGO_IMPORT_CONDITION_ENCRYPT_TYPE = "--mongo-import-condition-encrypt";
    protected final String MONGO_UPDATE_KEY = "--mongo-update-key";
    protected final String MONGO_IGNORE_UPDATE_KEY = "--mongo-ignore-update-key";
    protected final String MONGO_UPDATE_MODE = "--mongo-update-mode";
    protected final String MONGO_CAMEL_CONVERT = "--mongo-camel-convert";

    /**
     * hive launcher arguments.
     */
    protected final String HIVE_DATABASE = "--hive-database";
    protected final String HIVE_TABLE = "--hive-table";
    protected final String HIVE_AUTO_CREATE_TABLE = "--hive-auto-create-table";
    protected final String HIVE_DELETE_TABLEIF_EXISTS = "--hive-delete-table-if-exists";
    protected final String HIVE_FORMAT = "--hive-format";
    protected final String HIVE_PARTITION_KEY = "--hive-partition-key";
    protected final String HIVE_PARTITION_VALUE = "--hive-partition-value";
    protected final String HIVE_ENABLE_DYNAMIC = "--hive-enable-dynamic";
    protected final String HIVE_DYNAMIC_PARTITION_KEY = "--hive-dynamic-partition-key";
    protected final String HIVE_EXPORT_CONDITION = "--hive-export-condition";


    /**
     * Options that do not take arguments.
     */
    protected final String HELP = "--help";
    protected final String VERBOSE = "--verbose";
    protected final String VERSION = "--version";

    final String[][] opts = {
            {EXECUTION_ENGINE},
            {NAME},
            {CONNECT},
            {USER_NAME},
            {PASSWORD},
            {JAR_ADDR},
            {SPARK_HOME},
            {SPARK_MASTER},
            {SPARK_DEPLOY_MODE},
            {SPARK_CONF},
            {SPARK_PROPS_FILE},
            {SPARK_QUEUE},
            {SPARK_DRIVER_MEM},
            {SPARK_DRIVER_CORES},
            {SPARK_EXECUTOR_MEM},
            {SPARK_TOTAL_EXECUTOR_CORES},
            {SPARK_NUM_EXECUTORS},
            {SPARK_EXECUTOR_CORES},
            {MONGO_DATABASE},
            {MONGO_COLLECTION},
            {MONGO_EXTERNAL_CONF},
            {MONGO_IMPORT_CONDITION},
            {MONGO_IMPORT_CONDITION_ENCRYPT_TYPE},
            {MONGO_UPDATE_KEY},
            {MONGO_IGNORE_UPDATE_KEY},
            {MONGO_UPDATE_MODE},
            {MONGO_CAMEL_CONVERT},
            {HIVE_DATABASE},
            {HIVE_TABLE},
            {HIVE_AUTO_CREATE_TABLE},
            {HIVE_DELETE_TABLEIF_EXISTS},
            {HIVE_FORMAT},
            {HIVE_PARTITION_KEY},
            {HIVE_PARTITION_VALUE},
            {HIVE_ENABLE_DYNAMIC},
            {HIVE_DYNAMIC_PARTITION_KEY},
            {HIVE_EXPORT_CONDITION},
    };

    final String[][] switches = {
            {HELP, "-h"},
            {VERBOSE, "-v"},
            {VERSION},
    };


    /**
     * 解析参数信息
     *
     * @param args 程序输入参数
     */
    protected final void parse(List<String> args) {

        int idx;
        for (idx = 0; idx < args.size(); idx++) {
            String arg = args.get(idx);
            String value = null;

            Matcher m = eqSeparatedOpt.matcher(arg);
            if (m.matches()) {
                arg = m.group(1);
                value = m.group(2);
            }

            // Look for options with a value.
            String name = findCliOption(arg, opts);
            if (name != null) {
                if (value == null) {
                    if (idx == args.size() - 1) {
                        throw new IllegalArgumentException(
                                String.format("Missing argument for option '%s'.", arg));
                    }
                    idx++;
                    value = args.get(idx);
                }
                if (!handle(name, value)) {
                    break;
                }
                continue;
            }

            // Look for a switch.
            name = findCliOption(arg, switches);
            if (name != null) {
                if (!handle(name, null)) {
                    break;
                }
                continue;
            }

            if (!handleUnknown(arg)) {
                break;
            }
        }

        if (idx < args.size()) {
            idx++;
        }
    }

    protected boolean handle(String opt, String value) {
        throw new UnsupportedOperationException();
    }


    protected boolean handleUnknown(String opt) {
        throw new UnsupportedOperationException();
    }


    private String findCliOption(String name, String[][] available) {
        for (String[] candidates : available) {
            for (String candidate : candidates) {
                if (candidate.equals(name)) {
                    return candidates[0];
                }
            }
        }
        return null;
    }


}
