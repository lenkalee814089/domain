package com.dataexa.insight.domain.security.annotation;

import java.lang.annotation.*;

@Documented
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface InsightComponent {

    //组件名称
    String name() default "";

    //类型
    String type() default "com.dataexa.insight.dataprprocess";

    //描述
    String description() default "";

    //形状
    String shape() default "square-o";

    //图标
    String icon() default "circle-o";

    //高级组件
    boolean senior() default false;

    //序号
    int order() default 0;


}
