import org.testng.annotations.Test;

import java.util.Arrays;

/**
 * @author zach - 吸柒
 */
public class SortTest {

    @Test
    public void demo1 (){
      String  a = "A";
        String b = "B";
        int i = a.compareTo(b);
        System.out.println(i);

    }

    //F,A,D,I

    @Test
    public void sort (){

        String[] strs = new String[4];
        strs[0] = "F";
        strs[1] = "A";
        strs[2] = "D";
        strs[3] = "B";

        Arrays.sort(strs);

        System.out.println(Arrays.toString(strs));

    }


}
