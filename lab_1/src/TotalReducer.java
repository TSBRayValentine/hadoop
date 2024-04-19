import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;
import java.io.*;
import java.util.*;

/**
 * Класс реализует reduce-функцию для задачиhadoop
 */
public class TotalReducer
        extends Reducer<Text, IntWritable, Text, IntWritable> {

    /**
     * Метод выполняет reduce-функцию
     * 
     * @param key     ключ, передаваемый из map-функции
     * @param values  значения, передаваемые из map-функции
     * @param context контекст для записи результата
     */
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {

        // переменная, хранящая сумму значений
        int sum = 0;

        // перебираем все значения из map-функции
        for (IntWritable val : values) {

            // суммируем значения
            sum += val.get();
        }

        // записываем результат в контекст
        context.write(key, new IntWritable(sum));
    }
}
