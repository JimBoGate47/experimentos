import java.sql.Timestamp;
import java.time.LocalDateTime;

/**
 * Created by jsirpa on 02-12-16.
 */
public class Parser{
    private LocalDateTime localDate;
    public Parser(Timestamp localDate){
        this.localDate = localDate.toLocalDateTime();
    }
    public int M(){
        return this.localDate.getMonthValue();
    }

    public int Y(){
        return this.localDate.getYear();
    }

    public int D(){
        return this.localDate.getDayOfMonth();
    }
    public int DW(){
        return this.localDate.getDayOfWeek().getValue();
    }


}
