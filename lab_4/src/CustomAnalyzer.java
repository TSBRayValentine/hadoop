import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.StopwordAnalyzerBase;
import org.apache.lucene.analysis.pattern.PatternTokenizer;
import java.util.regex.Pattern;

/**
 * Класс анализатора, который будет использоваться для поиска в Lucene.
 * Описывает набор токенов (слова) для текста.
 */
public class CustomAnalyzer extends StopwordAnalyzerBase {
    /**
     * Конструктор, принимает набор стоп-слов.
     * 
     * @param stopwords список стоп-слов
     */
    public CustomAnalyzer(CharArraySet stopwords) {
        super(stopwords);
    }

    /**
     * Создает компонент для анализа текста: регулярный анализатор,
     * который разделяет текст на токены по правилам, описанным в регулярном
     * выражении.
     * 
     * @param s имя поля в Lucene, которое будет анализироваться
     * @return компонент для анализа текста
     */
    @Override
    protected TokenStreamComponents createComponents(String s) {
        return new TokenStreamComponents(new PatternTokenizer(
                Pattern.compile("([^_\\W]+-*)+|[.!?]+", Pattern.UNICODE_CHARACTER_CLASS), 0));
    }

}
