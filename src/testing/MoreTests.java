package testing;

import junit.framework.TestCase;
import org.junit.Test;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MoreTests extends TestCase {
    @Test
    public void testRegex() {
        assert(Pattern.matches("^[a-zA-Z]*$/g", "hithere"));
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
}
