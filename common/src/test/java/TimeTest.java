/**
 * <pre>
 *
 * Created by zhenqin.
 * User: zhenqin
 * Date: 16/10/18
 * Time: 17:31
 * Vendor: NowledgeData
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author zhenqin
 */
public class TimeTest {


    public static void main(String[] args) {
        int interval = 24 * 60 * 60 * 31;
        System.out.println(interval * 1000L);
        System.out.println(System.currentTimeMillis());
        System.out.println(System.currentTimeMillis() + interval * 1000L);
        long time = System.currentTimeMillis() + 24 * 60 * 60 * 1000 * 31;
        System.out.println(Integer.MAX_VALUE);
        System.out.println((int)time);
    }
}
