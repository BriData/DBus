package lib;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.stream.Collectors;

public class DBusKeeperAutoDeployStop {
    public static void main(String[] args) throws Exception {
        String[] lines = executeNormalCmd("jps -l").split("\n");
        for(String line: lines) {
            if (line.contains("gateway-0.5.0.jar")) {
                String[] ss = line.split(" ");
                System.out.println("发现gateway进程");
                executeNormalCmd("kill -9 " + ss[0]);
                System.out.println("结束gateway进程");
            } else if (line.contains("keeper-mgr-0.5.0.jar")) {
                String[] ss = line.split(" ");
                System.out.println("发现keeper-mgr进程");
                executeNormalCmd("kill -9 " + ss[0]);
                System.out.println("结束keeper-mgr进程");
            } else if (line.contains("keeper-service-0.5.0.jar")) {
                String[] ss = line.split(" ");
                System.out.println("发现keeper-service进程");
                executeNormalCmd("kill -9 " + ss[0]);
                System.out.println("结束keeper-service进程");
            } else if (line.contains("register-server-0.5.0.jar")) {
                String[] ss = line.split(" ");
                System.out.println("发现register-server进程");
                executeNormalCmd("kill -9 " + ss[0]);
                System.out.println("结束register-server进程");
            }
        }
    }

    private static String executeNormalCmd(String cmd) throws Exception {
        Process ps = Runtime.getRuntime().exec(cmd);
        ps.waitFor();
        return new BufferedReader(new InputStreamReader(ps.getInputStream()))
                .lines().parallel().collect(Collectors.joining("\n"));
    }
}
