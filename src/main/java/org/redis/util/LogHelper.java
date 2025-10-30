package org.redis.util;

/**
 * Утилитный класс для безопасной работы со строковыми представлениями объектов и обрезкой длинных строк.
 *
 * <p>Предназначен для логирования: позволяет избежать исключений при вызове {@code toString()},
 * а также ограничить длину выводимых строк для логов и отладочной информации.</p>
 */
public class LogHelper {

    /**
     * Безопасно преобразует объект в строку.
     *
     * <p>Если {@code toString()} выбрасывает исключение, возвращает строку вида
     * {@code "<toString-error:ТипИсключения>"} вместо падения приложения.</p>
     *
     * @param o объект для преобразования
     * @return строковое представление объекта или сообщение об ошибке
     */
    public static String safeToString(Object o) {
        try {
            return String.valueOf(o);
        } catch (Exception ex) {
            return "<toString-error:" + ex.getClass().getSimpleName() + ">";
        }
    }

    /**
     * Обрезает строку до заданной длины, добавляя пометку об усечении.
     *
     * <p>Если длина строки превышает {@code max}, возвращает первые {@code max} символов
     * с добавлением суффикса {@code "...(truncated)"}. Если строка короче или равна —
     * возвращается как есть.</p>
     *
     * @param s исходная строка
     * @param max максимальная допустимая длина
     * @return обрезанная строка или {@code null}, если вход был {@code null}
     */
    public static String truncate(String s, int max) {
        if (s == null) return null;
        return s.length() <= max ? s : s.substring(0, max) + "...(truncated)";
    }
}
