import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class WordCount implements Writable {

    // Слово в тексте
    private String word;

    // Количество вхождений слова в тексте
    private int freq;

    // Конструктор класса. Создает объект WordCount
    public WordCount(String word, int freq) {
        this.word = word;
        this.freq = freq;
    }

    // Сериализует объект в поток, используемый для передачи данных между узлами
    // Hadoop
    @Override
    public void write(DataOutput output) throws IOException {
        // Записывает слово в поток
        output.writeUTF(word);
        // Записывает количество вхождений слова в поток
        output.writeInt(freq);
    }

    // Десериализует объект из потока, используемого для передачи данных между
    // узлами Hadoop
    @Override
    public void readFields(DataInput input) throws IOException {
        // Считывает слово из потока
        word = input.readUTF();
        // Считывает количество вхождений слова из потока
        freq = input.readInt();
    }

    // Возвращает строковое представление объекта WordCount
    @Override
    public String toString() {
        return "word: " + word + "; frequency=" + freq;
    }

}
