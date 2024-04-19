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
     * Регулярное выражение для определения слова в тексте.
     */
    private static final Pattern isWord = Pattern.compile("^([^_\\W]+-*)+$",
            Pattern.UNICODE_CHARACTER_CLASS);

    /**
     * Метод маппера, который вызывается для каждого файла в директории.
     * 
     * @param key     ключ для хранения данных в редукци
     * @param value   текст, который нужно обработать
     * @param context контекст маппера, через который происходит запись данных
     * @throws IOException
     * @throws InterruptedException
     */
    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        // Объединение стоп-слов разных языков для одного списка
        CharArraySet englishStopWords = EnglishAnalyzer.getDefaultStopSet();
        CharArraySet frenchStopWords = FrenchAnalyzer.getDefaultStopSet();
        CharArraySet russianStopWords = RussianAnalyzer.getDefaultStopSet();
        CharArraySet germanStopWords = GermanAnalyzer.getDefaultStopSet();

        CharArraySet stopWords = new CharArraySet(englishStopWords, true);
        stopWords.addAll(englishStopWords);
        stopWords.addAll(frenchStopWords);
        stopWords.addAll(russianStopWords);
        stopWords.addAll(germanStopWords);

        // Настройка анализатора Lucene на использование списка стоп-слов
        CustomAnalyzer analyzer = new CustomAnalyzer(stopWords);

        // Получение потока токенов из Lucene
        TokenStream tokenStream = new StopFilter(analyzer.tokenStream("fieldName", value.toString()),
                stopWords);

        // Получение атрибута токена, хранящего текст токена
        CharTermAttribute attr = tokenStream.addAttribute(CharTermAttribute.class);
        String prev = "";
        tokenStream.reset();

        // Циклическое чтение токенов из потока
        while (tokenStream.incrementToken()) {
            String curr = attr.toString();

            // Если предыдущее и текущее слова являются словами, то запись в редукци
            if (!prev.isEmpty() && isWord.matcher(prev).matches() && isWord.matcher(curr).matches()) {
                context.write(new Text(prev.toLowerCase()), new Text(curr.toLowerCase()));
            }

            // Запоминание текущего слова для сравнения с предыдущим
            prev = curr;
        }
    }
}
