/**
 * @ author: create by LuJuHui
 * @ date:2018/8/10
 */

import java.io.InputStream;
import java.util.Properties;

/**
 * 配置管理组件
 * <p>
 * 1、配置管理组件可以复杂，也可以很简单，对于简单的配置管理组件来说，只要开发一个类，可以在第一次访问它的
 * 时候，就从对应的properties文件中，读取配置项，并提供外界获取某个配置key对应的value的方法
 * 2、如果是特别复杂的配置管理组件，那么可能需要使用一些软件设计中的设计模式，比如单例模式、解释器模式
 * 可能需要管理多个不同的properties，甚至是xml类型的配置文件
 * 3、我们这里的话，就是开发一个简单的配置管理组件，就可以了
 */
public class ConfigurationManager {

	// Properties对象使用private来修饰，就代表了其是类私有的
	// 那么外界的代码，就不能直接通过ConfigurationManager.prop这种方式获取到Properties对象
	// 之所以这么做，是为了避免外界的代码不小心错误的更新了Properties中某个key对应的value
	// 从而导致整个程序的状态错误，乃至崩溃
	private static Properties prop = new Properties();

	static {
		try {
			InputStream in = ConfigurationManager.class
					.getClassLoader().getResourceAsStream("conf.properties");
			prop.load(in);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * 获取指定key对应的value
	 *
	 * @param key
	 * @return value
	 */
	public static String getProperty(String key) {

		return prop.getProperty(key);
	}

	/**
	 * 获取整数类型的配置项
	 *
	 * @param key
	 * @return value
	 */
	public static Integer getInteger(String key) {
		String value = getProperty(key);
		try {
			return Integer.valueOf(value);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return 0;
	}

	/**
	 * 获取布尔类型的配置项
	 *
	 * @param key
	 * @return value
	 */
	public static Boolean getBoolean(String key) {
		String value = getProperty(key);
		try {
			return Boolean.valueOf(value);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return false;
	}

	/**
	 * 获取Long类型的配置项
	 *
	 * @param key
	 * @return
	 */
	public static Long getLong(String key) {
		String value = getProperty(key);
		try {
			return Long.valueOf(value);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return 0L;
	}
}