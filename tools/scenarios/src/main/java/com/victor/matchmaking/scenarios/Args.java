package com.victor.matchmaking.scenarios;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The flags of one command, parsed the way the shell calls it: {@code --name value} (and bare
 * values for the few commands that take them).
 *
 * <p>Deliberately not a framework: the harness has ~15 commands and a parser is 60 lines, while a
 * dependency would be one more thing to keep in step with the product's build.
 */
final class Args {

    private final Map<String, String> flags = new LinkedHashMap<>();
    private final List<String> positional = new ArrayList<>();

    private Args() {
    }

    static Args of(String[] argv) {
        Args parsed = new Args();
        for (int i = 0; i < argv.length; i++) {
            String token = argv[i];
            if (token.startsWith("--")) {
                String name = token.substring(2);
                if (i + 1 >= argv.length) {
                    throw new IllegalArgumentException("flag --" + name + " needs a value");
                }
                parsed.flags.put(name, argv[++i]);
            } else {
                parsed.positional.add(token);
            }
        }
        return parsed;
    }

    String required(String name) {
        String value = flags.get(name);
        if (value == null || value.isEmpty()) {
            throw new IllegalArgumentException("--" + name + " is required");
        }
        return value;
    }

    String optional(String name, String fallback) {
        String value = flags.get(name);
        return value == null ? fallback : value;
    }

    int intOf(String name, int fallback) {
        String value = flags.get(name);
        return value == null ? fallback : Integer.parseInt(value);
    }

    int requiredInt(String name) {
        String value = required(name);
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException notANumber) {
            throw new IllegalArgumentException("--" + name + " must be a number, got '" + value + "'");
        }
    }

    String positional(int index, String name) {
        if (index >= positional.size()) {
            throw new IllegalArgumentException(name + " is required");
        }
        return positional.get(index);
    }

    int positionalCount() {
        return positional.size();
    }
}
