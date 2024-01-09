package net.benchmarker.utils;

import java.util.HashMap;
import java.util.Map;

public class ArgumentParser {
    private final Map<String, String> arguments;

    public ArgumentParser(String[] args) {
        arguments = new HashMap<>();
        parseArguments(args);
    }

    private void parseArguments(String[] args) {
        String key = null;

        for (String arg : args) {
            if (arg.equals("-produce")) {
                arguments.put("produce", "true");
                continue;
            }

            if (arg.equals("-consume")) {
                arguments.put("consume", "true");
                continue;
            }

            if (arg.startsWith("-")) {
                key = arg.substring(1);
            } else {
                if (key != null) {
                    arguments.put(key, arg);
                    key = null;
                }
            }
        }
    }

    public String getArgument(String arg) {
        return arguments.get(arg);
    }
}
