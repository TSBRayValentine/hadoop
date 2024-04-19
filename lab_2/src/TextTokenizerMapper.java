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

public class TextTokenizerMapper extends Mapper<Object, Text, IntWritable, IntWritable> {
    // метод, который будет вызываться для каждого файла в директории
    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        // Сбор списка стоп-слов для всех языков
        CharArraySet englishStopWords = EnglishAnalyzer.getDefaultStopSet();
        CharArraySet frenchStopWords = FrenchAnalyzer.getDefaultStopSet();
        CharArraySet russianStopWords = RussianAnalyzer.getDefaultStopSet();
        CharArraySet germanStopWords = GermanAnalyzer.getDefaultStopSet();
        CharArraySet stopWords = new CharArraySet(englishStopWords, true);
        stopWords.addAll(englishStopWords);
        stopWords.addAll(frenchStopWords);
        stopWords.addAll(russianStopWords);
        stopWords.addAll(germanStopWords);
        // Создаем стандартный анализатор, который будет использоваться для потоковой
        // обработки текста
        StandardAnalyzer analyzer = new StandardAnalyzer(stopWords);
        // Получаем поток токенов из текста с помощью метода tokenStream
        TokenStream tokenStream = new StopFilter(analyzer.tokenStream("fieldName", value.toString()),
                analyzer.getStopwordSet());
        // Получаем информацию о текущем токене с помощью метода addAttribute
        CharTermAttribute attr = tokenStream.addAttribute(CharTermAttribute.class);
        // Сбрасываем поток токенов в начальное состояние
        tokenStream.reset();
        // Переходим по всем токенам и записываем их в контекст
        while (tokenStream.incrementToken()) {
            // Записываем в контекст длину текущего токена и количество вхождений этого
            // токена в документе
            context.write(new IntWritable(attr.length()), new IntWritable(1));
        }
    }
}
