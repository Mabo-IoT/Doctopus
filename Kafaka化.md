# 工作注意事项

1. [doctopus-dev](https://10.7.0.117:9091/mabo_group/base_application/doctopus-dev/tree/master)

   项目和之前的CI/CD流程不太一样，这里是直接加载到镜像中，没有考虑通过生成`whl`的方式。

   `whl`方式有以下好处：

   - 减少镜像空间
   - `whl`可以用于window的`doctopus`
   - 也可传递给外部人员

2. 将程序替换为具体文件，挂载的形式

   这样就不用搞ziyan的lua修改了

   但也引入了docker镜像挂掉的问题。

# 计划

4-17日搞定主CI/CD的项目

4-20日逐个更新具体项目的CI/CD

然后尝试一波自动触发。：D

