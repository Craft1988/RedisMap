package org.redis.util;

public class LogHelper {
    public static String safeToString(Object o) {
        try {
            return String.valueOf(o);
        } catch (Exception ex) {
            return "<toString-error:" + ex.getClass().getSimpleName() + ">";
        }
    }

    public static String truncate(String s, int max) {
        if (s == null) return null;
        return s.length() <= max ? s : s.substring(0, max) + "...(truncated)";
    }
}
