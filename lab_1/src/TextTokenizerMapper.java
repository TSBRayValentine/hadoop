import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.StopFilter;
import org.apache.lucene.analysis.StopwordAnalyzerBase;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.de.GermanAnalyzer;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.fr.FrenchAnalyzer;
import org.apache.lucene.analysis.ru.RussianAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import java.io.*;
import java.util.*;

/**
 * Класс маппера, который будет вызываться для каждого файла в директории,
 * содержащей текстовые документы.
 */
public class TextTokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

    /**
     * Метод, который будет вызываться для каждого файла.
     * 
     * @param key     ключ, соответствующий текущему документу
     * @param value   значение, соответствующее текущему документу
     * @param context объект, предоставляющий доступ к методам, необходимым для
     *                записи результатов маппера в Hadoop
     */
    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {

        /**
         * Создание списка стоп-слов на основе списка стоп-слов для английского,
         * французского и немецкого языков.
         */
        CharArraySet englishStopWords = EnglishAnalyzer.getDefaultStopSet();
        CharArraySet frenchStopWords = FrenchAnalyzer.getDefaultStopSet();
        CharArraySet russianStopWords = RussianAnalyzer.getDefaultStopSet();
        CharArraySet germanStopWords = GermanAnalyzer.getDefaultStopSet();
        CharArraySet stopWords = new CharArraySet(englishStopWords, true);
        stopWords.addAll(englishStopWords);
        stopWords.addAll(frenchStopWords);
        stopWords.addAll(russianStopWords);
        stopWords.addAll(germanStopWords);

        /**
         * Создание стандартного анализатора с набором стоп-слов.
         */
        StandardAnalyzer analyzer = new StandardAnalyzer(stopWords);

        /**
         * Создание потока токенов на основе анализатора и текущего документа.
         */
        TokenStream tokenStream = new StopFilter(analyzer.tokenStream("fieldName", value.toString()),
                analyzer.getStopwordSet());

        /**
         * Получение атрибута типа CharTermAttribute из токенов в потоке.
         */
        CharTermAttribute attr = tokenStream.addAttribute(CharTermAttribute.class);

        /**
         * Сброс потока токенов, чтобы снова считать с начала.
         */
        tokenStream.reset();

        /**
         * Цикл по всем токенам в потоке.
         */
        while (tokenStream.incrementToken()) {

            /**
             * Запись текущего токена и значения "1" в контекст.
             */
            context.write(new Text(attr.toString()), new IntWritable(1));
        }
    }
}
