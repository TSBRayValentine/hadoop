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

    // точка входа в приложение, запускает hadoop job
    public static void main(String[] args) throws Exception {
        // запуск hadoop job
        int ret = ToolRunner.run(new Configuration(), new HadoopDriver(), args);
        // выход из приложения
        System.exit(ret);
    }

    // метод, который запускается hadoop job'ом (определяется в main)
    public int run(String[] args) throws Exception {

        // проверка числа аргументов командной строки
        if (args.length != 2) {
            // вывод сообщения об ошибке в stderr
            ToolRunner.printGenericCommandUsage(System.err);
            // вывод сообщения об ошибке в stderr
            System.err.println("USAGE: hadoop jar ... <input-dir> <output-dir>");
            // выход с ошибкой
            System.exit(1);
        }

        // создание объекта Job, который будет запущен hadoop job'ом
        Job job = Job.getInstance(getConf());
        // указание класса, который будет использоваться при создании конфигурации для
        // hadoop job'а
        job.setJarByClass(HadoopDriver.class);
        // задание имени для hadoop job'а
        job.setJobName("WordCounter");
        // добавление входного пути для чтения данных
        FileInputFormat.addInputPath(job, new Path(args[0]));
        // задание выходного пути для записи результатов
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        // указание класса, который будет использоваться для парсинга данных входного
        // потока
        job.setMapperClass(TextTokenizerMapper.class);
        // указание класса, который будет использоваться для слияния данных, полученных
        // из маппера
        job.setReducerClass(TotalReducer.class);
        // указание типа ключа, который будет передаваться в маппер
        job.setMapOutputKeyClass(Text.class);
        // указание типа значения, которое будет передаваться в маппер
        job.setMapOutputValueClass(Text.class);

        // вывод в stdout информации о входных путях и выходном пути hadoop job'а
        System.out.println("Input dirs: " + Arrays.toString(FileInputFormat.getInputPaths(job)));
        System.out.println("Output dir: " + FileOutputFormat.getOutputPath(job));

        // запуск hadoop job'а и ожидание его завершения
        return job.waitForCompletion(true) ? 0 : 1;
    }

}
