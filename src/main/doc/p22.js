/*
 * 定义一个move()函数来执行一些简单的动画效果
 * 参数：
 * 	obj 要执修改的属性
 * 	target 执行动行动画的对象
 * 	attr 执行动画时要画的目标位置
 * 	speed 动画执行的速度
 *  callback 回调函数，当动画执行完毕以后，该回调函数会执行
 */
function move(obj, attr, target, speed, callback) {
    //关闭之前的定时器
    /*
     * 一般都会将定时器的标识作为执行动画对象的属性保存，这样可以确保只有当前对象能访问到定时器
     */
    clearInterval(obj.timer);

    //判断向左移还是向右移
    //0 --> 800 向右移
    //起始位置 < 目标位置 则向右移动，速度为正
    //800 --> 0 向左移
    //起始位置 > 目标位置 则向左移动，速度为负

    //获取元素的起始位置
    var current = parseInt(getStyle(obj, attr));

    if(current > target) {
        //起始位置 > 目标位置 则向左移动，速度为负
        speed = -speed;
    }

    //开启一个定时器，控制box1移动
    obj.timer = setInterval(function() {

        //获取box1的当前的left值
        var oldValue = parseInt(getStyle(obj, attr));

        //通过旧值来计算新值
        var newValue = oldValue + speed;

        //判断newValue是否大于800
        /*
         * 如果从0 向 800，执行动画，则值越来越大
         * 如果从800向0执行动画，则值越来小
         */
        if((speed > 0 && newValue > target) || (speed < 0 && newValue < target)) {
            newValue = target;
        }

        //将box1的left值修改为新值
        obj.style[attr] = newValue + "px";

        //当box1移动到800px的位置时，停止移动
        if(newValue == target) {

            clearInterval(obj.timer);

            //调用回调函数
            callback && callback();
        }

    }, 30);

}

/*
 * 用来获取任意元素的当前样式
 * 	参数：
 * 		obj 要获取样式的元素
 * 		name 要获取的样式的名字
 *
 * 在读取元素的样式时，如果元素没有设置样式，
 * 	则火狐、Chrome等浏览器会自动计算元素的样式值
 * 	而IE浏览器，有时用不会自动计算，而是返回默认值，比如宽度会返回auto
 *
 */
function getStyle(obj, name) {

    //判断浏览器中是否含有getComputedStyle这个方法
    if(window.getComputedStyle) {
        //支持正常的浏览器
        return getComputedStyle(obj, null)[name];
    } else {
        //只支持IE
        return obj.currentStyle[name];
    }

}

/*
 * 定义一个专门用来向元素中添加class的函数
 * 	参数：
 * 		obj 要添加class属性的对象
 * 		cn 要添加的class的属性值
 */
function addClass(obj, cn) {

    //如果元素中有该class则不添加，没有才添加
    if(!hasClass(obj, cn)) {
        obj.className += " " + cn;
    }

}

/*
 * 创建一个函数检查一个元素中是否含有指定的class
 * 	如果有，则返回true。否则返回false
 */
function hasClass(obj, cn) {

    //创建正则表达式
    var reg = new RegExp("\\b" + cn + "\\b");

    //返回检查结果
    return reg.test(obj.className);
}

/*
 * 用来从指定元素中删除class值的方法
 */
function removeClass(obj, cn) {

    //要删除一个class，就是将这个class替换为一个空串
    //创建正则表达式
    var reg = new RegExp("\\b" + cn + "\\b", "g");

    //判断obj中是否含有这个class

    if(reg.test(obj.className)) {
        //将内容替换为空串
        obj.className = obj.className.replace(reg, "");
    }
}

/*
 * 用来切换元素的class的方法
 * 	如果元素中含有该class，则删除
 * 	如果元素中没有该class，则添加
 *
 */
function toggleClass(obj, cn) {
    //检查obj中是否含有cn
    if(hasClass(obj, cn)) {
        //有该class，则删除
        removeClass(obj, cn);
    } else {
        //没有该class，则添加
        addClass(obj, cn);
    }

}