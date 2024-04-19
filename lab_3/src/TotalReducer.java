import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

/**
 * Класс реализует reduce-функцию для задачиhadoop.
 * Входные данные к reduce-функции - пакет пар (ключ, значение),
 * выходные данные - пакет пар (ключ, значение).
 */
public class TotalReducer extends Reducer<Text, Text, Text, Text> {
    /**
     * Реализует reduce-функцию для задачиhadoop.
     * 
     * @param key     Ключ, который группирует значения
     * @param values  Итератор значений, которые соответствуют ключу
     * @param context Контекст, который передается в метод
     * @throws IOException
     * @throws InterruptedException
     */
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        // Создаем словарь, в котором будет храниться количество вхождений
        // каждого слова в пакете значений
        Map<Text, Integer> freqs = new HashMap<>();
        // Проходим по каждому значению и увеличиваем счетчик в словаре для
        // этого значения или создаем новый элемент в словаре с счетчиком равным 1
        for (Text value : values) {
            freqs.put(value, freqs.getOrDefault(value, 0) + 1);
        }
        // Находим наиболее часто встречающееся слово в пакете значений
        Text mostFreq = freqs.entrySet().stream()
                // Сортируем словарь по значению счетчика и находим максимальный счетчик
                .max(Map.Entry.comparingByValue())
                // Получаем ключ наиболее часто встречающегося слова
                .map(Map.Entry::getKey)
                // Если в пакете не было ни одного значения, то возвращаем
                // специальный ключ, который обозначает изолированное слово
                .orElse(new Text("@isolated word@"));

        // Записываем результат в контекст задачи
        context.write(key, mostFreq);
    }
}
