/**
 * Created by jsirpa on 23-11-16.
 */
public class JavaRecord implements java.io.Serializable {
    private String word;
    private String word1;

    public void setWord(String word) {
        this.word = word;
    }
    public void setWord1(String wordc) {
        this.word1 = wordc;
    }
    public String getWord() {
        return word;
    }
    public String getWord1() {
        return word1;
    }


}