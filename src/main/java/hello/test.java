package hello;

import java.awt.datatransfer.SystemFlavorMap;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by dfinch on 3/7/16.
 */
public class test {
    public static void main(){
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss-SS");
        try {
            Date time = format.parse("2011-09-22 17:10:20-17");

            System.out.println("Table init:");
            //Get locationid for station name "Foster NB" (1049)

            //Get detectorids for stationid (1377,1378,1379)

            //Get loopdata for September 22, 2011 7-9AM and 4-6PM
            Date morningStart = format.parse("2011-09-22 07:00:00-00");
            Date morningEnd = format.parse("2011-09-22 09:00:00-59");
            Date rushStart = format.parse("2011-09-22 16:00:00-00");
            Date rushEnd = format.parse("2011-09-22 18:00:00-59");

            if ( time.equals(morningStart) ||  (time.after(morningStart) && time.before(morningEnd)) || time.equals(morningEnd)) {
                System.out.println("Morning Peak Speed:");
            }
            else {
                System.out.println("nope");
            }

            if (time.equals(rushStart) || (time.after(rushStart) && time.before(rushEnd)) || time.equals(rushEnd)) {
                System.out.println("Rush Peak Speed:");
            }
            else {
                System.out.println("nope2");
            }
        }
        catch(Exception e) {

        }
    }
}
