package lib;
import java.io.FileInputStream;
import java.util.Properties;

public class DBusKeeperAutoDeployInit {
    private static Properties pro;
    public static void main(String[] args) throws Exception {
        System.out.println("加载config文件...");
        pro = new Properties();
        FileInputStream in = new FileInputStream("config.properties");
        pro.load(in);
        in.close();

        System.out.println("解压gateway...");
        executeNormalCmd("unzip -q lib/gateway-0.5.0.jar");
        System.out.println("配置gateway...");
        replaceTemplate(
                "./BOOT-INF/classes/application-opensource.yaml",
                "eureka.client.serviceUrl.defaultZone"
        );
        System.out.println("压缩gateway...");
        executeNormalCmd("rm lib/gateway-0.5.0.jar");
        executeNormalCmd("zip -qr0 lib/gateway-0.5.0.jar BOOT-INF/ META-INF/ org/");
        executeNormalCmd("rm -r BOOT-INF/ META-INF/ org/");


        System.out.println("解压keeper-mgr...");
        executeNormalCmd("unzip -q lib/keeper-mgr-0.5.0.jar");
        System.out.println("配置keeper-mgr...");
        replaceTemplate(
                "./BOOT-INF/classes/application-opensource.yaml",
                "eureka.client.serviceUrl.defaultZone"
        );
        replaceTemplate(
                "./BOOT-INF/classes/application-opensource.yaml",
                "zk.str"
        );
        System.out.println("压缩keeper-mgr...");
        executeNormalCmd("rm lib/keeper-mgr-0.5.0.jar");
        executeNormalCmd("zip -qr0 lib/keeper-mgr-0.5.0.jar BOOT-INF/ META-INF/ org/");
        executeNormalCmd("rm -r BOOT-INF/ META-INF/ org/");


        System.out.println("解压keeper-service...");
        executeNormalCmd("unzip -q lib/keeper-service-0.5.0.jar");
        System.out.println("配置keeper-service...");
        replaceTemplate(
                "./BOOT-INF/classes/application-opensource.yaml",
                "spring.datasource.password"
        );
        replaceTemplate(
                "./BOOT-INF/classes/application-opensource.yaml",
                "spring.datasource.driver-class-name"
        );
        replaceTemplate(
                "./BOOT-INF/classes/application-opensource.yaml",
                "spring.datasource.url"
        );
        replaceTemplate(
                "./BOOT-INF/classes/application-opensource.yaml",
                "spring.datasource.username"
        );
        replaceTemplate(
                "./BOOT-INF/classes/application-opensource.yaml",
                "eureka.instance.metadataMap.alarmEmail"
        );
        replaceTemplate(
                "./BOOT-INF/classes/application-opensource.yaml",
                "eureka.client.serviceUrl.defaultZone"
        );
        replaceTemplate(
                "./BOOT-INF/classes/application-opensource.yaml",
                "zk.str"
        );
        System.out.println("压缩keeper-service...");
        executeNormalCmd("rm lib/keeper-service-0.5.0.jar");
        executeNormalCmd("zip -qr0 lib/keeper-service-0.5.0.jar BOOT-INF/ META-INF/ org/");
        executeNormalCmd("rm -r BOOT-INF/ META-INF/ org/");
        System.out.println("初始化完成");
    }

    private static void replaceTemplate(String filepath, String key) throws Exception {
        String cmd = String.format("sed -i 's/#{%s}#/%s/g' %s", key, pro.getProperty(key), filepath);
        Process ps = Runtime.getRuntime().exec(cmd);
        ps.waitFor();
    }
    private static void executeNormalCmd(String cmd) throws Exception {
        Process ps = Runtime.getRuntime().exec(cmd);
        ps.waitFor();
    }
}
