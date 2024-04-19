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
    // основной входной точка приложения - точка входа в JVM,
    // тут мы инициализируем конфигурацию и запускаем код программы
    public static void main(String[] args) throws Exception {
        // запуск приложения, передача конфигурации и самого приложения
        int ret = ToolRunner.run(new Configuration(), new HadoopDriver(), args);
        // завершение приложения с кодом возврата, который вернул Job
        System.exit(ret);
    }

    // метод, который реализует интерфейс Tool, в нем происходит настройка и запуск
    // MapReduce задачи
    public int run(String[] args) throws Exception {
        // проверка на правильность аргументов командной строки
        if (args.length != 2) {
            // вывод информации о том, как корректно запустить приложение
            ToolRunner.printGenericCommandUsage(System.err);
            System.err.println("USAGE: hadoop jar ... <input-dir> <output-dir>");
            // завершение приложения с кодом ошибки
            System.exit(1);
        }
        // создание конфигурации для задачи
        Job job = Job.getInstance(getConf());
        // указание класса, который будет запускаться в JVM,
        // т.е. точка входа в программу
        job.setJarByClass(HadoopDriver.class);
        // название задачи
        job.setJobName("WordCounter");
        // добавление входного пути для MapReduce задачи
        FileInputFormat.addInputPath(job, new Path(args[0]));
        // добавление выходного пути для MapReduce задачи
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        // указание класса, который будет использоваться в качестве Mapper'a
        job.setMapperClass(TextTokenizerMapper.class);
        // указание класса, который будет использоваться в качестве Reducer'a
        job.setReducerClass(TotalReducer.class);
        // указание типа ключа для выходных данных
        job.setMapOutputKeyClass(Text.class);
        // указание типа значения для выходных данных
        job.setMapOutputValueClass(Text.class);
        // вывод информации о входных и выходных путях задачи
        System.out.println("Input dirs: " + Arrays.toString(FileInputFormat.getInputPaths(job)));
        System.out.println("Output dir: " + FileOutputFormat.getOutputPath(job));
        // запуск задачи, если задача выполнилась успешно, то возвращается 0, иначе 1
        return job.waitForCompletion(true) ? 0 : 1;
    }

}
