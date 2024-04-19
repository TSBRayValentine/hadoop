import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;
import java.io.*;
import java.util.*;

public class HadoopDriver extends Configured implements Tool {
    // Главная точка входа в программу. Запуск задачи через hadoop jar
    public static void main(String[] args) throws Exception {
        // запуск задачи, передача конфигурации и объекта класса HadoopDriver
        int ret = ToolRunner.run(new Configuration(), new HadoopDriver(), args);
        // выход с кодом возврата
        System.exit(ret);
    }

    // Запуск задачи. Реализация интерфейса Tool
    public int run(String[] args) throws Exception {
        // проверка количества аргументов
        if (args.length != 2) {
            // печать стандартной строки помощи
            ToolRunner.printGenericCommandUsage(System.err);
            // вывод ошибки в консоль
            System.err.println("USAGE: hadoop jar ... <input-dir> <output-dir>");
            // выход с ошибкой
            System.exit(1);
        }
        // создание объекта задачи
        Job job = Job.getInstance(getConf());
        // указание jar-файла, который содержит классы задачи
        job.setJarByClass(HadoopDriver.class);
        // указание имени задачи
        job.setJobName("WordCounter");
        // добавление входного каталога для чтения
        FileInputFormat.addInputPath(job, new Path(args[0]));
        // добавление выходного каталога для записи результата
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        // указание класса маппера
        job.setMapperClass(TextTokenizerMapper.class);
        // указание класса редуктора
        job.setReducerClass(TotalReducer.class);
        // указание классов выходных значений маппера и класса ключа редуктора
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntWritable.class);
        // печать в консоль информации об input и output
        System.out.println("Input dirs: " + Arrays.toString(FileInputFormat.getInputPaths(job)));
        System.out.println("Output dir: " + FileOutputFormat.getOutputPath(job));
        // запуск задачи и ожидание ее завершения
        return job.waitForCompletion(true) ? 0 : 1;
    }

}
