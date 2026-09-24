package edu.usp.cs324.client;

import edu.usp.cs324.api.Job;
import java.util.*;

/** Numeric CSV: commas/newlines, optional numeric quotes, no headers or empty cells. */
public final class JobInput {
    private JobInput() { }
    public static Job parse(Job.Type type, String text) {
        List<Integer> values = new ArrayList<>();
        String normalized = text.startsWith("\uFEFF") ? text.substring(1) : text;
        for (String line : normalized.split("\\R")) {
            if (line.isBlank()) continue;
            for (String cell : line.split(",", -1)) {
                String value = cell.trim();
                if (value.length() >= 2 && value.startsWith("\"") && value.endsWith("\"")) {
                    value = value.substring(1, value.length() - 1).trim();
                }
                if (!value.matches("[+-]?[0-9]+")) {
                    throw new IllegalArgumentException("Each CSV cell must contain an integer: '" + cell + "'");
                }
                try { values.add(Integer.parseInt(value)); }
                catch (NumberFormatException e) {
                    throw new IllegalArgumentException("Integer outside -2147483648..2147483647: " + value, e);
                }
            }
        }
        if (type == Job.Type.PRIMESUM) {
            if (values.size() != 2) throw new IllegalArgumentException("PRIMESUM requires exactly start,end");
            return new Job(type, List.of(), values.get(0), values.get(1));
        }
        return new Job(type, values, 0, 0);
    }
}
