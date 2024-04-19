import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.StopwordAnalyzerBase;
import org.apache.lucene.analysis.pattern.PatternTokenizer;
import java.util.regex.Pattern;

/**
 * Класс анализатора, который будет использоваться для
 * разделения текста на отдельные слова.
 */
public class CustomAnalyzer extends StopwordAnalyzerBase {
    /**
     * Конструктор, который принимает набор стоп-слов.
     * 
     * @param stopwords набор стоп-слов
     */
    public CustomAnalyzer(CharArraySet stopwords) {
        super(stopwords);
    }

    /**
     * Метод, который будет вызываться для создания
     * компонентов для обработки каждого файла.
     * 
     * @param s имя файла
     * @return компоненты для обработки текста
     */
    @Override
    protected TokenStreamComponents createComponents(String s) {
        return new TokenStreamComponents(new PatternTokenizer(Pattern.compile("([^_\\W]+-*)+|[.!?]+",
                Pattern.UNICODE_CHARACTER_CLASS), 0));
    }
}
