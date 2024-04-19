import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

public class TotalReducer
        extends Reducer<Text, Text, Text, WordCount> {
    /**
     * Метод, вызываемый для обработки группы значений с одинаковым ключом
     * 
     * @param key     ключ
     * @param values  итератор значений с данным ключом
     * @param context контекст выполнения задачи
     * @throws IOException          исключения ввода-вывода
     * @throws InterruptedException прерывание выполнения задачи
     */
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        // словарь счетчиков частоты слов
        Map<Text, Integer> freqs = new HashMap<>();
        // для каждого значения с текущим ключом
        for (Text value : values) {
            // увеличить счетчик для этого значения на 1, либо установить значение 1, если
            // его нет в словаре
            freqs.put(value, freqs.getOrDefault(value, 0) + 1);
        }
        // получить максимальный элемент и соответствующее ему значение
        WordCount mostFreq = freqs.entrySet().stream()
                // сравнение по значению
                .max(Map.Entry.comparingByValue())
                // если нет максимального элемента, то создать его на основе ключа
                .map(it -> new WordCount(key.toString(), it.getKey().toString(), it.getValue()))
                .orElse(new WordCount(key.toString(), "@isolated word@", 0));

        context.write(key, mostFreq);
    }
}
