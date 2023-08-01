package com.bazaaring.snowflake

import org.springframework.boot.context.properties.ConfigurationProperties

@ConfigurationProperties("spring.snowflake")
class SnowflakeProperties {

    var datacenter = 0
    var k8s = K8s()

    class K8s {
        var deployment = ""
    }

}
