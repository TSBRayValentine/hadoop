import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

public class TotalReducer
        extends Reducer<Text, Text, Text, WordCount> {
    /**
     * Функция reduce выполняет reduce-функцию для задачи.
     * Входные данные к reduce-функции - пакет пар (ключ, значение),
     * выходные данные - пакет пар (ключ, значение).
     *
     * @param key     ключ, который группирует пары (ключ, значение)
     * @param values  итератор значений, относящихся к ключу
     * @param context контекст, предоставляющий доступ к методам для записи выходных
     *                данных
     * @throws IOException
     * @throws InterruptedException
     */
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        // Создание хэш-карты для подсчета частоты вхождения слов в тексте
        Map<Text, Integer> freqs = new HashMap<>();
        // Проход по всем значениям, относящимся к ключу
        for (Text value : values) {
            // Увеличение счетчика для символьного значения
            freqs.put(value, freqs.getOrDefault(value, 0) + 1);
        }
        // Нахождение наиболее часто встречающегося слова
        WordCount mostFreq = freqs.entrySet().stream()
                .max(Map.Entry.comparingByValue())
                // Преобразование найденной пары к WordCount
                .map(it -> new WordCount(it.getKey().toString(), it.getValue()))
                // Выбор значения по умолчанию, если все слова из группы изолированы
                .orElse(new WordCount("@isolated word@", 0));

        context.write(key, mostFreq);
    }
}
