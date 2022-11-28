## DataStream 算子API

> 1. 作用于单个事件的基本转换
> 2. 针对相同键值的keyedStream转换
> 3. 将多条数据流合并为一条或将一条数据流拆分多条流的转换
> 4. 对流中事件进行重新组织的分发转换.

### 基础1. Map [#](https://nightlies.apache.org/flink/flink-docs-release-1.15/zh/docs/dev/datastream/operators/overview/#map)

#### DataStream → DataStream [#](https://nightlies.apache.org/flink/flink-docs-release-1.15/zh/docs/dev/datastream/operators/overview/#datastream-rarr-datastream)

输入一个元素同时输出一个元素。下面是将输入流中元素数值加倍的 map function：

```java
DataStreamSource<Event> ds = ....
ds.map(e -> e.getUrl());
```

### 基础2. FlatMap [#](https://nightlies.apache.org/flink/flink-docs-release-1.15/zh/docs/dev/datastream/operators/overview/#flatmap)

#### DataStream → DataStream [#](https://nightlies.apache.org/flink/flink-docs-release-1.15/zh/docs/dev/datastream/operators/overview/#datastream-rarr-datastream-1)

输入一个元素同时产生零个、一个或多个元素。下面是将句子拆分为单词的 flatmap function：

```java
        ds.flatMap(new FlatMapFunction<Event, List<String> >() {
            @Override
            public void flatMap(Event value, Collector<List<String>> out) throws Exception {
                List<String> split = Arrays.asList(value.user.split(""));
                out.collect(split);
            }
        }).print();

        ds.flatMap(((Event value, Collector<List<String>> collector) -> {
            List<String> split = Arrays.asList(value.user.split(""));
            collector.collect(split);
        })).returns(new TypeHint<List<String>>() {}).print();

        ds.flatMap((Event e,Collector<CallName> c) -> {
           c.collect( new CallName(e.getUser()) );
        }).returns(TypeInformation.of(CallName.class)).print();

注意:lambada比匿名内部类, 在运行时执行invokedynamic指令,即是在第一次执行逻辑才会确定✅,而不是真正编译为class文件, 会有 type erasure 问题 The generic type parameters of Collector are missing, 无法确定返回类型.
为了克服引入typē hit 在returns 引入.
```

### 基础3. Filter [#](https://nightlies.apache.org/flink/flink-docs-release-1.15/zh/docs/dev/datastream/operators/overview/#filter)

#### DataStream → DataStream [#](https://nightlies.apache.org/flink/flink-docs-release-1.15/zh/docs/dev/datastream/operators/overview/#datastream-rarr-datastream-2)

为每个元素执行一个布尔 function，并保留那些 function 输出值为 true 的元素。下面是过滤掉零值的 filter：

```java
        ds.filter(new FilterFunction<Event>() {
            @Override
            public boolean filter(Event value) throws Exception {
                return value.getUrl().equals("./home");
            }
        }).print();
```

### 基于键值: KeyBy [#](https://nightlies.apache.org/flink/flink-docs-release-1.15/zh/docs/dev/datastream/operators/overview/#keyby)

#### DataStream → KeyedStream [#](https://nightlies.apache.org/flink/flink-docs-release-1.15/zh/docs/dev/datastream/operators/overview/#datastream-rarr-keyedstream)

在逻辑上将流划分为不相交的分区。具有相同 key 的记录都分配到同一个分区。在内部， *keyBy()* 是通过哈希分区实现的。有多种[指定 key ](https://nightlies.apache.org/flink/flink-docs-release-1.15/zh/docs/dev/datastream/fault-tolerance/state/#keyed-datastream)的方式。注意不要使用持续增长的ID作为键值,避免出现内存问题.

```java
			ds.keyBy(new KeySelector<Event, String>() {
            @Override
            public String getKey(Event value) throws Exception {
                return value.url;
            }
        });
```

### 基于键值: 滚动聚合 [#](https://nightlies.apache.org/flink/flink-docs-release-1.15/zh/docs/dev/datastream/operators/overview/#reduce)

#### KeyedStream → DataStream [#](https://nightlies.apache.org/flink/flink-docs-release-1.15/zh/docs/dev/datastream/operators/overview/#keyedstream-rarr-datastream)

在相同 key 的数据流上“滚动”执行 算子。滚动聚合算子会为每个处理过的键值维持一个状态,由于状态不会被清理,所以

不要用在无限数据流.