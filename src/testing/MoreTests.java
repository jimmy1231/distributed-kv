package testing;

import junit.framework.TestCase;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MoreTests extends TestCase {
    private static final List<Integer> BIN_RANGES = Arrays.asList(
        0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100,
        110, 120, 130, 140, 150, 160, 170, 180,
        190, 200, 210, 220, 230, 240, 255
    );

    @Test
    public void testRegex() {
        assert(Pattern.matches("^[a-zA-Z]*$", "hitheremynameisjimmyhithere"));
        // String to be scanned to find the pattern.
        String line = "This order was placed for QT3000! OK?";
        String pattern = "(.*)(\\d+)(.*)";

        // Create a Pattern object
        Pattern r = Pattern.compile(pattern);

        // Now create matcher object.
        Matcher m = r.matcher(line);
        if (m.find( )) {
            System.out.println("Found value: " + m.group(0) );
            System.out.println("Found value: " + m.group(1) );
            System.out.println("Found value: " + m.group(2) );
        }else {
            System.out.println("NO MATCH");
        }
    }

    @Test
    public void testByte() {
        String str = "d there!";

        int byteVal = ((int) str.getBytes(StandardCharsets.US_ASCII)[0]) & 0xFF;
        System.out.println(byteVal);

        Integer binId;
        int lower, upper;
        for (binId=0; binId<BIN_RANGES.size()-1; binId++) {
            lower = BIN_RANGES.get(binId);
            upper = BIN_RANGES.get(binId+1);

            if (byteVal >= lower && byteVal < upper) {
                System.out.println(binId);
                break;
            }
        }
    }
}
