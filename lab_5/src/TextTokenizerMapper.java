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

public class TextTokenizerMapper extends Mapper<Object, Text, Text, Text> {
    /**
     * Регулярное выражение для определения того, что символьная строка содержит
     * только слова, разделенные дефисом.
     */
    private static final Pattern isWord = Pattern.compile("^([^_\\W]+-*)+$",
            Pattern.UNICODE_CHARACTER_CLASS);

    /**
     * Метод, вызываемый для каждой пары (ключ, значение) входных данных,
     * доступных в рамках задачи map-reduce.
     *
     * @param key     ключ входных данных, в нашем случае не используется
     * @param value   значение входных данных, содержащее текстовый документ
     * @param context контекст, позволяющий записывать пары (ключ, значение)
     * @throws IOException          исключение, которое может быть вызвано при
     *                              работе с входными данными или выходными данными
     * @throws InterruptedException исключение, которое может быть вызвано при
     *                              прерывании задачи
     */
    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        // Создаем список стоп-слов для каждого языка
        CharArraySet englishStopWords = EnglishAnalyzer.getDefaultStopSet();
        CharArraySet frenchStopWords = FrenchAnalyzer.getDefaultStopSet();
        CharArraySet russianStopWords = RussianAnalyzer.getDefaultStopSet();
        CharArraySet germanStopWords = GermanAnalyzer.getDefaultStopSet();

        // Объединяем все стоп-слова в один список
        CharArraySet stopWords = new CharArraySet(englishStopWords, true);
        stopWords.addAll(englishStopWords);
        stopWords.addAll(frenchStopWords);
        stopWords.addAll(russianStopWords);
        stopWords.addAll(germanStopWords);

        // Инициализируем наш анализатор с использованием созданного списка стоп-слов
        CustomAnalyzer analyzer = new CustomAnalyzer(stopWords);

        // Создаем поток токенов для анализа текста
        TokenStream tokenStream = new StopFilter(analyzer.tokenStream("fieldName", value.toString()),
                stopWords);

        // Получаем атрибут, который будет содержать токены
        CharTermAttribute attr = tokenStream.addAttribute(CharTermAttribute.class);

        // Для хранения предыдущего токена
        String prev = "";

        // Сбрасываем поток токенов, чтобы начать его проход с начала
        tokenStream.reset();

        // Проходим по всем токенам, до тех пор, пока не будет достигнут конец потока
        while (tokenStream.incrementToken()) {
            // Получаем текущий токен
            String curr = attr.toString();

            // Если предыдущий и текущий токены - слова, то записываем их в контекст
            if (!prev.isEmpty() && isWord.matcher(prev).matches() && isWord.matcher(curr).matches()) {
                context.write(new Text(prev.toLowerCase()), new Text(curr.toLowerCase()));
            }

            // Сохраняем текущий токен как предыдущий
            prev = curr;
        }
    }
}
