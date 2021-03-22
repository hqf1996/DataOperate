import java.util.*;

/**
 * @Author: yhj
 * @Description:
 * @Date: Created in 2018/7/5.
 */
public class MapSortTest {
    public static void main(String[] args) {
        Map<String, Integer> map = new HashMap<>();
        map.put("a", 2);
        map.put("c", 3);
        map.put("d", 5);
        map.put("f", 7);
        map.put("g", 8);
        map.put("k", 5);
        map.put("y", 6);
        List<Map.Entry<String, Integer>> list = new ArrayList<>(map.entrySet());
        Collections.sort(list, new Comparator<Map.Entry<String, Integer>>() {
            @Override
            public int compare(Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2) {
                return o1.getValue() - o2.getValue();
            }
        });
        for(Map.Entry<String, Integer> entry: list){
            System.out.println(entry.getKey() + " " + entry.getValue());
        }
    }
}
