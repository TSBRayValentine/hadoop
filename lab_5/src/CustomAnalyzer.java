import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.StopwordAnalyzerBase;
import org.apache.lucene.analysis.pattern.PatternTokenizer;
import java.util.regex.Pattern;

/**
 * Класс анализатора, который будет использоваться для Lucene.
 * Реализует настройки для разделения текста на отдельные слова.
 */
public class CustomAnalyzer extends StopwordAnalyzerBase {

    /**
     * Конструктор класса.
     * 
     * @param stopwords набор стоп-слов
     */
    public CustomAnalyzer(CharArraySet stopwords) {
        super(stopwords);
    }

    /**
     * Создает компоненты для анализатора:
     * токенизатор и фильтр стоп-слов.
     * 
     * @param s пустой строкой
     * @return компоненты для анализатора
     */
    @Override
    protected TokenStreamComponents createComponents(String s) {
        return new TokenStreamComponents(new PatternTokenizer(
                // регулярное выражение для разделения текста на отдельные слова
                // ([^_\\W]+-*) – один или более символов, не являющихся символами
                // разделения слов и не являющихся символами конца строки или
                // начала строки, также как и символы пунктуации, за исключением
                // дефиса и символа-разделителя
                // (+) – один или более раз
                // |[.!?]+ – или символы пунктуации
                Pattern.compile("([^_\\W]+-*)+|[.!?]+", Pattern.UNICODE_CHARACTER_CLASS),
                0));
    }
}
