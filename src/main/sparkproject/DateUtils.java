/**
 * @ author: create by LuJuHui
 * @ date:2018/8/10
 */
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * 时间日期工具类
 * */
public class DateUtils {

	public static final SimpleDateFormat TIME_FORMAT =
			new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	public static final SimpleDateFormat DATE_FORMAT =
			new SimpleDateFormat("yyyy-MM-dd");
	public static final SimpleDateFormat DATEKEY_FORMAT =
			new SimpleDateFormat("yyyyMMdd");

	/**
	 * 判断第一个时间是否在第二个时间之前
	 * */
	public static boolean before(String firstTime,String secondTime){
		try {
			Date first = TIME_FORMAT.parse(firstTime);
			Date second = TIME_FORMAT.parse(secondTime);

			if(first.before(second)){
				return true;
			}
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return false;
	}

	/**
	 * 判断第一个时间是否在第二个时间之后
	 * */
	public static boolean after(String firstTime,String secondTime){
		try {
			Date first = TIME_FORMAT.parse(firstTime);
			Date second = TIME_FORMAT.parse(secondTime);

			if(first.after(second)){
				return true;
			}
		}catch (ParseException e){
			e.printStackTrace();
		}
		return false;
	}

	/**
	 * 计算2个时间的差值(单位为秒)
	 * */
	public static int minus(String firstTime,String secondTime){
		try {
			Date first = TIME_FORMAT.parse(firstTime);
			Date second = TIME_FORMAT.parse(secondTime);
			long millisecond = first.getTime() - second.getTime();
			return Integer.valueOf(String.valueOf(millisecond/1000));
		}catch (ParseException e){
			e.printStackTrace();
		}
		return 0;
	}

	/**
	 * 获取年月日和小时
	 * */
	public static String getDateHour(String datetime){
		String date = datetime.split(" ")[0];
		String hourMinuteSecond = datetime.split(" ")[1];
		String hour = hourMinuteSecond.split(":")[0];
		return date+"_"+hour;
	}

	/**
	 * 获取当天的日期
	 * */
	public static String getTodayDate(){
		return DATE_FORMAT.format(new Date());
	}

	/**
	 * 获取昨天的日期
	 * */
	public static String getYesterdayDate(){
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(new Date());
		calendar.add(Calendar.DAY_OF_YEAR,-1);

		Date time = calendar.getTime();
		return DATE_FORMAT.format(time);
	}

	/**
	 * 格式化日期（yyyy-MM-dd）
	 */
	public static String formatDate(Date date) {
		return DATE_FORMAT.format(date);
	}

	/**
	 * 格式化时间（yyyy-MM-dd HH:mm:ss）
	 */
	public static String formatTime(Date date) {
		return TIME_FORMAT.format(date);
	}

	/**
	 * 解析时间字符串
	 */
	public static Date parseTime(String time) {
		try {
			return TIME_FORMAT.parse(time);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return null;
	}

	/**
	 * 格式化日期key
	 */
	public static String formatDateKey(Date date) {
		return DATEKEY_FORMAT.format(date);
	}

	/**
	 * 格式化日期key
	 */
	public static Date parseDateKey(String datekey) {
		try {
			return DATEKEY_FORMAT.parse(datekey);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return null;
	}

	/**
	 * 格式化时间，保留到分钟级别
	 */
	public static String formatTimeMinute(Date date) {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmm");
		return sdf.format(date);
	}
}
