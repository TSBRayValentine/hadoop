import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;
import java.io.*;
import java.util.*;

/**
 * Класс-драйвер для запуска задания на Hadoop с помощью ToolRunner.
 */
public class HadoopDriver extends Configured implements Tool {

    /**
     * Точка входа в приложение.
     * 
     * @param args Аргументы командной строки.
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        // Запуск задания и получение его возврата
        int ret = ToolRunner.run(new Configuration(), new HadoopDriver(), args);
        // Завершение приложения
        System.exit(ret);
    }

    /**
     * Метод, вызываемый при запуске задания на Hadoop.
     * 
     * @param args Аргументы командной строки.
     * @return Код возврата задания (0 - успешное завершение).
     * @throws Exception
     */
    public int run(String[] args) throws Exception {
        // Проверка количества аргументов
        if (args.length != 2) {
            // Вывод сообщения об ошибке и завершение приложения
            ToolRunner.printGenericCommandUsage(System.err);
            System.err.println("USAGE: hadoop jar ... <input-dir> <output-dir>");
            System.exit(1);
        }
        // Создание задания
        Job job = Job.getInstance(getConf());
        // Установка класса-драйвера задания
        job.setJarByClass(HadoopDriver.class);
        // Установка имени задания
        job.setJobName("WordCounter");
        // Добавление входного пути
        FileInputFormat.addInputPath(job, new Path(args[0]));
        // Добавление выходного пути
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        // Установка класса маппера
        job.setMapperClass(TextTokenizerMapper.class);
        // Установка класса редуктора
        job.setReducerClass(TotalReducer.class);
        // Установка классов входного и выходного ключей
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        // Вывод информации о задании
        System.out.println("Input dirs: " + Arrays.toString(FileInputFormat.getInputPaths(job)));
        System.out.println("Output dir: " + FileOutputFormat.getOutputPath(job));
        // Запуск задания и ожидание его завершения
        return job.waitForCompletion(true) ? 0 : 1;
    }

}
