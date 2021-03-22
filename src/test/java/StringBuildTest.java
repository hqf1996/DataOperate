/**
 * @Author: yhj
 * @Description:
 * @Date: Created in 2018/7/5.
 */
public class StringBuildTest {
    public static void main(String[] args) {
        StringBuilder stringBuilder = new StringBuilder("大赛,发发顺丰,法是否，");
        stringBuilder.deleteCharAt(stringBuilder.length()-1);
        System.out.print(stringBuilder);
    }
}
