import java.util.Map;

/**
 * Created by jsirpa on 23-11-16.
 */
//public class JavaRecord implements java.io.Serializable {
public class JavaRecord {
    private String word1;
    private String word2;
    private String word3;

    public void setWord1(String word) {
        this.word1 = word;
    }
    public void setWord2(String word) {
        this.word2 = word;
    }
    public void setWord3(String test) {
        this.word3 = test;
    }
    public String getWord1() {
        return word1;
    }
    public String getWord2() {
        return word2;
    }
    public String getWord3() {
        return word3;
    }


}