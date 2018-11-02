package com.dataexa.data.prepare.annotation;

import java.lang.annotation.*;

@Documented
@Target(ElementType.PARAMETER)
@Retention(RetentionPolicy.RUNTIME)
public @interface InsightComponentArg {

    //外部输入
    boolean externalInput() default false;

    //只读
    boolean readOnly() default false;

    //高级
    boolean senior() default false;

    //名称
    String name() default "";

    //描述
    String description() default "";

    //是否必填
    boolean request() default false;

    //默认值
    String defaultValue() default "";

    //可选值，用分号分隔
    String items() default "";
}
