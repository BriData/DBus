

欢迎来到Dbus doc文档



DBus docs说明文档以md文件提供，可使用专门的工具编译成HTML文档，方便浏览。

编译md文件需要安装ruby 2.4.x, msys2，jekyll等工具。

下面以window安装、编译为例，进行说明。

### 1 安装Ruby 2.4.x和msys2
先下载ruby 2.4.x 
https://github.com/oneclick/rubyinstaller2/releases/download/rubyinstaller-2.4.3-1/rubyinstaller-2.4.3-1-x64.exe

双击打开，安装ruby。注意：安装路径不能有空格；记住选中选项“添加到path”。

安装ruby执行后，会自动跳出msys2的安装界面。

按回车键，把msys2 1 2 3 都装上。  

#####如果msys2 安装失败，需要手动安装
手动下载msys2安装文件：http://repo.msys2.org/distrib/x86_64/msys2-x86_64-20161025.exe 

双击打开，安装。


安装完成后， 需在cmd中执行下面的命令，进一步安装 msys2相关的一些组件。逐个下载、安装想组件，会持续较长的时间。从cmd界面能看到当前正在下载安装的组件及进度。
```sh
	c:\> ridk install
```

安装完成后，界面显示如下内容，表明安装成功：
```sh
	Install MSYS2 and MINGW development toolchain succeeded
	
	   1 - MSYS2 base installation
	   2 - MSYS2 system update
	   3 - MSYS2 and MINGW development toolchain
	
	Which components shall be installed? If unsure press ENTER []
```
### 2 安装jekyll

```sh
c:\> gem install jekyll jekyll-redirect-from
```

### 3 生成DBus文档

```sh
D:\code\dbus\dbus-main>cd docs

D:\code\dbus\dbus-main\docs>jekyll build SKIP_API=1
```

在 _site目录可看到编译生成的html文件。
