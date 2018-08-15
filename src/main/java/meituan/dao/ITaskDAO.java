package meituan.dao;

import meituan.domain.Task;

/**
 * @ author: create by LuJuHui
 * @ date:2018/8/13
 */
public interface ITaskDAO {
	/**
	 * 根据主键ID查询任务
	 *
	 * @param taskid 主键
	 * @return 任务
	 */
	Task findById(long taskid);
}
