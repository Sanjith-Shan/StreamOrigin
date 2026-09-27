package io.streamorigin.packager;

import java.util.HashMap;
import java.util.Map;

/** Minimal {@code --key value} parser; flags without a value read as "true". */
final class Args {

    private final Map<String, String> values = new HashMap<>();
    final String command;

    Args(String[] argv) {
        String cmd = "publish";
        int i = 0;
        if (argv.length > 0 && !argv[0].startsWith("--")) {
            cmd = argv[0];
            i = 1;
        }
        command = cmd;
        for (; i < argv.length; i++) {
            if (!argv[i].startsWith("--")) {
                throw new IllegalArgumentException("unexpected argument " + argv[i]);
            }
            String key = argv[i].substring(2);
            if (i + 1 < argv.length && !argv[i + 1].startsWith("--")) {
                values.put(key, argv[++i]);
            } else {
                values.put(key, "true");
            }
        }
    }

    String str(String key, String def) {
        return values.getOrDefault(key, def);
    }

    long lng(String key, long def) {
        return values.containsKey(key) ? Long.parseLong(values.get(key)) : def;
    }

    double dbl(String key, double def) {
        return values.containsKey(key) ? Double.parseDouble(values.get(key)) : def;
    }

    boolean flag(String key) {
        return Boolean.parseBoolean(values.getOrDefault(key, "false"));
    }
}
