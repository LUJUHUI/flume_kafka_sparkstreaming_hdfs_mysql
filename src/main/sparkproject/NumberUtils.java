/**
 * @ author: create by LuJuHui
 * @ date:2018/8/10
 */
import java.math.BigDecimal;

/**
 * 数字格式化工具类
 */
public class NumberUtils {

	/**
	 * 格式化小数
	 * @param num 字符串
	 * @param scale 四舍五入的位数
	 * @return 格式化小数
	 */
	public static double formatDouble(double num, int scale) {
		BigDecimal bd = new BigDecimal(num);
		return bd.setScale(scale, BigDecimal.ROUND_HALF_UP).doubleValue();
	}

}
