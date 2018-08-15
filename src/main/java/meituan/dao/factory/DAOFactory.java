package meituan.dao.factory;

import meituan.dao.ITaskDAO;
import meituan.dao.impl.TaskDAOImpl;

/**
 * @ author: create by LuJuHui
 * @ date:2018/8/13
 */
public class DAOFactory {
	/**
	 * 获取任务管理DAO
	 * @return DAO
	 */
	public static ITaskDAO getTaskDAO() {
		return new TaskDAOImpl();
	}
}
