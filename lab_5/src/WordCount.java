import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class WordCount implements Writable {

    // Слово в тексте
    private String word;
    // Следующее слово в тексте
    private String next;
    // Количество вхождений слова в тексте
    private int freq;

    // Конструктор класса WordCount
    public WordCount(String word, String next, int freq) {
        this.word = word;
        this.next = next;
        this.freq = freq;
    }

    // Сериализует объект в поток, используемый для передачи данных между узлами
    // Hadoop
    @Override
    public void write(DataOutput output) throws IOException {
        output.writeUTF(word); // Записывает слово в поток
        output.writeChars(next); // Записывает следующее слово в поток
        output.writeInt(freq); // Записывает количество вхождений слова в поток
    }

    // Десериализует объект из потока, используемого для передачи данных между
    // узлами Hadoop
    @Override
    public void readFields(DataInput input) throws IOException {
        word = input.readUTF(); // Считывает слово из потока
        next = input.readUTF(); // Считывает следующее слово из потока
        freq = input.readInt(); // Считывает количество вхождений слова из потока
    }

    // Возвращает строковое представление объекта WordCount
    @Override
    public String toString() {
        return "word: " + word + "next: " + next + "; frequency=" + freq;
    }

    // Реализует сравнение объектов WordCount
    // Возвращает 0 если объекты равны, <0 если текущий объект меньше переданного,
    // >0 если текущий объект больше переданного
    public int compareTo(WordCount o) {
        if (o == null) {
            return 1; // Если переданный объект равен null, то текущий объект больше null
        }
        if (this.freq != o.freq) { // Сравниваем количество вхождений
            return this.freq - o.freq; // Возвращаем разницу в количестве вхождений
        }
        if (!this.word.equals(o.word)) { // Сравниваем слова
            return this.word.compareTo(o.word); // Возвращаем разницу между словами
        }
        if (!this.next.equals(o.next)) { // Сравниваем следующие слова
            return this.next.compareTo(o.next); // Возвращаем разницу между следующими словами
        }
        return 0; // Если все поля равны, то объекты равны
    }

}
