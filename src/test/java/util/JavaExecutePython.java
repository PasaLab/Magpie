package util;

import org.junit.Before;
import org.junit.Test;
import org.python.core.*;
import org.python.util.PythonInterpreter;
/**
 * jython库测试
 * @author mengfeiyang
 *
 */
public class JavaExecutePython {
    public PythonInterpreter interpreter ;
    public String basePath = JavaExecutePython.class.getResource("").getPath();
    @Before
    public void start(){
        interpreter = new PythonInterpreter();
    }

    //在java类中直接执行python语句
    @Test
    public void test01() {
        interpreter.exec("days=('mod','Tue','Wed','Thu','Fri','Sat','Sun'); ");
        interpreter.exec("print days[1];");
    }

    //在java中调用本机python脚本中的函数
    @Test
    public void test02(){
        interpreter.execfile("D:\\idea\\project\\华为\\AutoConf\\src\\main\\resources\\test1.py");
        PyFunction func = (PyFunction) interpreter.get("adder",PyFunction.class);

        int a = 2010, b = 2;
        PyObject pyobj = func.__call__(new PyInteger(a), new PyInteger(b));
        System.out.println("anwser = " + pyobj.toString());
    }

    //直接执行Python脚本
    @Test
    public void test03(){
        PythonInterpreter interpreter = new PythonInterpreter();

        PySystemState sys = Py.getSystemState();
        interpreter.execfile("D:\\idea\\project\\华为\\AutoConf\\src\\main\\java\\Util\\Baye.py");
    }
}