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
    // основная точка входа в приложение
    public static void main(String[] args) throws Exception {
        // запуск приложения с помощью ToolRunner и получение результата
        int ret = ToolRunner.run(new Configuration(), new HadoopDriver(), args);
        // завершение приложения с кодом возврата
        System.exit(ret);
    }

    // запуск задачи из jar-файла
    public int run(String[] args) throws Exception {
        // проверка на правильность числа аргументов
        if (args.length != 2) {
            // вывод инструкции по использованию приложения
            ToolRunner.printGenericCommandUsage(System.err);
            System.err.println("USAGE: hadoop jar ... <input-dir> <output-dir>");
            // завершение приложения с кодом ошибки
            System.exit(1);
        }
        // создание задачи
        Job job = Job.getInstance(getConf());
        // указание класса, который будет использоваться в качестве jar-файла
        job.setJarByClass(HadoopDriver.class);
        // задает имя задачи
        job.setJobName("WordCounter");
        // добавление входного пути задачи
        FileInputFormat.addInputPath(job, new Path(args[0]));
        // задает выходной путь задачи
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        // задает класс маппера
        job.setMapperClass(TextTokenizerMapper.class);
        // задает класс редуктора
        job.setReducerClass(TotalReducer.class);
        // задает классы для выходных данных маппера и редуктора
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        // вывод в консоль входных путей и выходного пути задачи
        System.out.println("Input dirs: " + Arrays.toString(FileInputFormat.getInputPaths(job)));
        System.out.println("Output dir: " + FileOutputFormat.getOutputPath(job));
        // запуск задачи и ожидание ее завершения. возвращает true, если задача
        // завершилась успешно
        return job.waitForCompletion(true) ? 0 : 1;
    }

}
