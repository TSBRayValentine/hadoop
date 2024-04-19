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
import java.util.regex.Pattern;

/**
 * Класс маппера, который будет вызываться для каждого файла в директории,
 * содержащей текстовые документы.
 */
public class TextTokenizerMapper extends Mapper<Object, Text, Text, Text> {
    /**
     * Регулярное выражение для определения, что строка содержит только
     * буквы и знаки тире.
     */
    private static final Pattern isWord = Pattern.compile("^([^_\\W]+-*)+$",
            Pattern.UNICODE_CHARACTER_CLASS);

    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        /**
         * Создаем набор стоп-слов для английского, французского и
         * немецкого языков.
         */
        CharArraySet englishStopWords = EnglishAnalyzer.getDefaultStopSet();
        CharArraySet frenchStopWords = FrenchAnalyzer.getDefaultStopSet();
        CharArraySet russianStopWords = RussianAnalyzer.getDefaultStopSet();
        CharArraySet germanStopWords = GermanAnalyzer.getDefaultStopSet();

        /**
         * Объединяем все стоп-слова в один набор.
         */
        CharArraySet stopWords = new CharArraySet(englishStopWords, true);
        stopWords.addAll(englishStopWords);
        stopWords.addAll(frenchStopWords);
        stopWords.addAll(russianStopWords);
        stopWords.addAll(germanStopWords);

        /**
         * Создаем пользовательский анализатор, который будет использоваться
         * для преобразования строки в набор токенов.
         */
        CustomAnalyzer analyzer = new CustomAnalyzer(stopWords);

        /**
         * Создаем поток токенов на основе строки, которая была передана
         * в метод.
         */
        TokenStream tokenStream = new StopFilter(analyzer.tokenStream("fieldName", value.toString()),
                stopWords);

        /**
         * Извлекаем атрибут CharTermAttribute из токенового потока.
         */
        CharTermAttribute attr = tokenStream.addAttribute(CharTermAttribute.class);

        /**
         * Задаем начальное значение предыдущей строки.
         */
        String prev = "";

        /**
         * Сбрасываем токеновый поток для начала его чтения.
         */
        tokenStream.reset();

        /**
         * Итерация по всем токенам в потоке.
         */
        while (tokenStream.incrementToken()) {

            /**
             * Извлекаем текущую строку из потока.
             */
            String curr = attr.toString();

            /**
             * Если предыдущая и текущая строки являются словами, то записываем их
             * в контекст.
             */
            if (!prev.isEmpty() && isWord.matcher(prev).matches() && isWord.matcher(curr).matches()) {
                context.write(new Text(prev.toLowerCase()), new Text(curr.toLowerCase()));
            }

            /**
             * Заменяем предыдущую строку текущей.
             */
            prev = curr;
        }
    }
}
