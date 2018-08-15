package lib;

import java.io.File;
import java.io.FileInputStream;
import java.io.PrintWriter;
import java.util.Properties;
import java.util.Scanner;

public class DBusKeeperAutoDeployInit {
    private static Properties pro;

    public static void main(String[] args) throws Exception {
        System.out.println("加载config文件...");
        pro = new Properties();
        FileInputStream in = new FileInputStream("config.properties");
        pro.load(in);
        in.close();

        System.out.println("新建logs目录...");
        executeNormalCmd("mkdir -p logs");

        initGateway();
        initMgr();
        initService();

        System.out.println("初始化完成");
    }

    private static void initService() throws Exception {
        System.out.println("解压keeper-service...");
        executeNormalCmd("unzip -oq lib/keeper-service-0.5.0.jar");
        executeNormalCmd("cp -f ./BOOT-INF/classes/application-opensource.yaml ./BOOT-INF/classes/application.yaml");
        System.out.println("配置keeper-service...");
        replaceTemplate("./BOOT-INF/classes/application.yaml");
        System.out.println("压缩keeper-service...");
        executeNormalCmd("rm lib/keeper-service-0.5.0.jar");
        executeNormalCmd("zip -qr0 lib/keeper-service-0.5.0.jar BOOT-INF/ META-INF/ org/");
        executeNormalCmd("rm -r BOOT-INF/ META-INF/ org/");
    }

    private static void initMgr() throws Exception {
        System.out.println("解压keeper-mgr...");
        executeNormalCmd("unzip -oq lib/keeper-mgr-0.5.0.jar");
        executeNormalCmd("cp -f ./BOOT-INF/classes/application-opensource.yaml ./BOOT-INF/classes/application.yaml");
        System.out.println("配置keeper-mgr...");
        replaceTemplate("./BOOT-INF/classes/application.yaml");
        System.out.println("压缩keeper-mgr...");
        executeNormalCmd("rm lib/keeper-mgr-0.5.0.jar");
        executeNormalCmd("zip -qr0 lib/keeper-mgr-0.5.0.jar BOOT-INF/ META-INF/ org/");
        executeNormalCmd("rm -r BOOT-INF/ META-INF/ org/");
    }

    private static void initGateway() throws Exception {
        System.out.println("解压gateway...");
        executeNormalCmd("unzip -oq lib/gateway-0.5.0.jar");
        executeNormalCmd("cp -f ./BOOT-INF/classes/application-opensource.yaml ./BOOT-INF/classes/application.yaml");
        System.out.println("配置gateway...");
        replaceTemplate("./BOOT-INF/classes/application.yaml");
        System.out.println("压缩gateway...");
        executeNormalCmd("rm lib/gateway-0.5.0.jar");
        executeNormalCmd("zip -qr0 lib/gateway-0.5.0.jar BOOT-INF/ META-INF/ org/");
        executeNormalCmd("rm -r BOOT-INF/ META-INF/ org/");
    }

    private static void replaceTemplate(String filepath) throws Exception {
        Scanner scanner = new Scanner(new File(filepath));
        StringBuilder sb = new StringBuilder();
        while (scanner.hasNextLine()) {
            String s = scanner.nextLine();
            for (String key : pro.stringPropertyNames()) {
                s = s.replace("#{" + key + "}#", pro.getProperty(key));
            }
            sb.append(s).append(System.getProperty("line.separator"));
        }
        scanner.close();
        PrintWriter printWriter = new PrintWriter(new File(filepath));
        printWriter.print(sb.toString());
        printWriter.close();
    }

    private static void executeNormalCmd(String cmd) throws Exception {
        Process ps = Runtime.getRuntime().exec(cmd);
        ps.waitFor();
    }
}
