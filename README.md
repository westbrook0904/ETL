# ETL
抽取组件过滤条件的组合模式设计
设计背景
在ETL管道的转换环节，不同字段往往需要根据多样化的业务规则进行映射转换，如：常量赋值、默认值、原值引用、数学运算、自定义函数等。为支撑多种计算规则并便于后续扩展，采用策略模式结合枚举类进行建模，实现计算逻辑的高度解耦和动态扩展。

2. 关键模型与机制设计
2.1 枚举类定义
MappingType枚举：明确描述各类映射计算类型（如CONSTANT、DEFAULT、SOURCE、ARITHMETIC、CUSTOM_FUNCTION等），为策略查找提供依据。

2.2 策略接口定义
ValueCalculator接口

定义标准方法calculate(context)，所有计算类型统一实现此方法。

输入参数通常为转换上下文，输出为计算后的值。

2.3 各类策略实现
例如：

ConstantValueCalculator：常量值映射

DefaultValueCalculator：默认值赋值

SourceValueCalculator：原字段值

ArithmeticValueCalculator：四则运算

CustomFunctionValueCalculator：自定义函数

实现要点

每个策略类实现ValueCalculator接口，封装独立计算逻辑。

实现InitializingBean接口，使得在IOC容器初始化完成后自动注册到工厂。

2.4 策略工厂模式
CalculatorFactory工厂类

负责所有策略类注册和查找。

提供getCalculator(MappingType)方法，根据映射类型获取对应策略组件。

注册方法为protected，仅包内可见，防止被外部误用，提高封装性和安全性。

3. 类结构与流程图建议
3.1 UML类图
lua
复制
编辑
                    +----------------------+
                    |  ValueCalculator     |
                    +----------------------+
                    | +calculate(ctx):val  |
                    +----------------------+
                              ^
            +----------------+-------------------------------+
            |        |         |        |                    |
+----------------+ ... +------------------+    +-----------------------+
| ConstantValue  |     | DefaultValue     |    | ArithmeticValue...    |
| Calculator     |     | Calculator       |    | CustomFunction...     |
+----------------+     +------------------+    +-----------------------+
            ^                      ^                         ^
            |                      |                         |
         +-------------------------------------------------------+
         |        InitializingBean（IOC注册生命周期）            |
         +-------------------------------------------------------+

+-------------------------------------------------------+
| CalculatorFactory                                     |
+-------------------------------------------------------+
| - Map<MappingType, ValueCalculator> registry          |
+-------------------------------------------------------+
| + getCalculator(type: MappingType): ValueCalculator   |
| # register(type, calculator)                          |
+-------------------------------------------------------+
3.2 策略选择与执行流程
根据配置或业务需求获取映射类型（MappingType）。

通过CalculatorFactory.getCalculator(MappingType)查找并获取对应策略组件。

调用策略的calculate(context)方法，执行具体映射计算逻辑。

返回转换后的值用于后续处理。

4. 设计优势分析
解耦与扩展性：各类计算策略独立封装，新增计算类型仅需扩展枚举及实现接口，无需影响现有逻辑。

IOC自动注册：利用生命周期回调自动注册，避免手工维护映射关系。

安全性和封装：工厂注册方法受保护，仅限包内使用，防止外部误操作。

统一调用接口：无论何种计算类型，外部只需通过统一接口获取并调用，简化调用方逻辑。

便于测试和维护：策略类单一职责，便于单元测试与局部维护。

5. 总结
本设计通过枚举与策略模式的结合，极大提升了转换组件的灵活性与扩展性，有效支撑了复杂多样的字段映射需求，实现了解耦、可维护、可扩展和安全的设计目标。


