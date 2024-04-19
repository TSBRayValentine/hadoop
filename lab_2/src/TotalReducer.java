import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;
import java.io.*;
import java.util.*;

public class TotalReducer // класс реализует reduce-функцию для задачиhadoop
        extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> { // наследуется от класса Reducer, в
                                                                              // котором указано
                                                                              // типы ключей и значений

    public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException { // метод reduce принимает на вход значения с одинаковым ключом
        int sum = 0; // итератор значений и контекст
        for (IntWritable val : values) { // перебираем значения
            sum += val.get(); // суммируем их
        }
        context.write(key, new IntWritable(sum)); // записываем результат в контекст
    } // для дальнейшей обработки
}
